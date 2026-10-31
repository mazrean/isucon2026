package isureal

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// Group commit for ISUREAL_ASIA_LOCALIMG (asia1 keeps post / RealMoji /
// profile images on its own disk). Each keep writes data + meta without any
// fsync, then waits for the next sync of the shared syncer: one goroutine that
// takes every waiter registered so far and makes all their files durable with
// a single syncfs(2) on the store directory (file data, inodes and the
// rename/create directory entries — the whole filesystem). Writers that arrive
// while a sync runs join the next one, so under the GP burst (~850 posts in
// ~8s) one syncfs covers many posts instead of 4 fsyncs per post.
// keep returns only after its group's sync finished, so the caller commits and
// answers 201 only for images that survive a crash (same contract as before).
//
// ISUREAL_ASIA_LOCALIMG_SYNC=file restores the per-file fsync path (A/B).

var asiaLocalImgPerFileSync = os.Getenv("ISUREAL_ASIA_LOCALIMG_SYNC") == "file"

type syncGroup struct {
	done  chan struct{}
	err   error
	n     int
	paths []string // only used by the non-syncfs fallback
}

type groupSyncer struct {
	once    sync.Once
	mu      sync.Mutex
	cur     *syncGroup
	kick    chan struct{}
	dir     func() string
	syncFn  func() error // nil = syncfs(dir) (ASIA_LOCALIMG)。store の WAL は自分のファイルだけ fdatasync する
	batches atomic.Int64
	members atomic.Int64
}

var asiaSyncer = &groupSyncer{dir: func() string { return asiaStore.dir }}

// wait blocks until a sync that started after this call (so after every write
// the caller already did to paths) has completed.
func (g *groupSyncer) wait(paths ...string) error {
	g.once.Do(func() {
		g.kick = make(chan struct{}, 1)
		go g.loop()
	})
	g.mu.Lock()
	if g.cur == nil {
		g.cur = &syncGroup{done: make(chan struct{})}
	}
	grp := g.cur
	grp.n++
	grp.paths = append(grp.paths, paths...)
	g.mu.Unlock()
	select {
	case g.kick <- struct{}{}:
	default:
	}
	<-grp.done
	return grp.err
}

func (g *groupSyncer) loop() {
	for range g.kick {
		g.mu.Lock()
		grp := g.cur
		g.cur = nil // later writers go to the next group
		g.mu.Unlock()
		if grp == nil {
			continue
		}
		if g.syncFn != nil {
			grp.err = g.syncFn()
		} else {
			grp.err = syncStoreDir(g.dir(), grp.paths)
		}
		g.batches.Add(1)
		g.members.Add(int64(grp.n))
		close(grp.done)
	}
}

// writeFileNoSync writes path atomically (temp + rename) without fsync; the
// group sync makes the data and the directory entries durable.
func writeFileNoSync(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	_, werr := f.Write(data)
	cerr := f.Close()
	if werr != nil || cerr != nil {
		os.Remove(f.Name())
		return fmt.Errorf("localimg write %s: %v %v", path, werr, cerr)
	}
	if err := os.Rename(f.Name(), path); err != nil {
		os.Remove(f.Name())
		return err
	}
	return nil
}

// syncFilesAndDirs is the portable fallback: fsync every file in parallel,
// then every distinct parent directory.
func syncFilesAndDirs(paths []string) error {
	var wg sync.WaitGroup
	errs := make(chan error, 2*len(paths))
	dirs := map[string]struct{}{}
	for _, p := range paths {
		dirs[filepath.Dir(p)] = struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- fsyncPath(p)
		}()
	}
	wg.Wait()
	for d := range dirs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- fsyncPath(d)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func fsyncPath(p string) error {
	f, err := os.Open(p)
	if err != nil {
		return err
	}
	err = f.Sync()
	f.Close()
	return err
}
