// Copyright (c) 2026 Denis Babchuk
//
// This file is part of Silo Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio/internal/amztime"
	"github.com/minio/minio/internal/bucket/lifecycle"
	objectlock "github.com/minio/minio/internal/bucket/object/lock"
	"github.com/minio/minio/internal/bucket/replication"
	"github.com/minio/minio/internal/bucket/versioning"
	xhttp "github.com/minio/minio/internal/http"
)

// Expiring the current version of a versioned object only puts a delete marker
// on top of it: the version stays as a noncurrent version, and decommission
// and rebalance have to move it like any other. A version that ILM deletes by
// itself is deleted by the walk through the expiry path, which re-evaluates the
// current rule, so it is not moved, and the source copies of the versions that
// were moved are still removed. A deletion of every version of the object is
// left to the expiry workers.

// migrationILM is a bucket in pool 0 of two pools. The ILM expiry workers are
// replaced by a queue that nothing drains, so the expiries a decommission or
// rebalance queues run after it, as when the workers lag behind the walk.
type migrationILM struct {
	t       *testing.T
	z       *erasureServerPools
	bucket  string
	queue   chan expiryOp
	objects []string
	labels  map[string]string // object and version ID -> label of a version
	markers map[string]string // object and version ID -> label of a delete marker
}

func newMigrationILM(t *testing.T, status versioning.State, lock bool, rules string) *migrationILM {
	t.Helper()
	z, _ := migrationTestPools(t)
	ctx := t.Context()
	m := &migrationILM{
		t: t, z: z, bucket: "migration-ilm", queue: make(chan expiryOp, 100),
		labels: map[string]string{}, markers: map[string]string{},
	}
	err := z.MakeBucket(ctx, m.bucket, MakeBucketOptions{LockEnabled: lock, VersioningEnabled: status == versioning.Enabled})
	if err != nil {
		t.Fatal(err)
	}
	meta, err := globalBucketMetadataSys.Get(m.bucket)
	if err != nil {
		t.Fatal(err)
	}
	if status == versioning.Suspended {
		meta.VersioningConfigXML = []byte("<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>")
	}
	if rules != "" {
		meta.LifecycleConfigXML = []byte("<LifecycleConfiguration>" + rules + "</LifecycleConfiguration>")
	}
	if err := meta.Save(ctx, z); err != nil {
		t.Fatal(err)
	}
	globalBucketMetadataSys.Set(m.bucket, meta)

	es := &expiryState{ctx: ctx, objAPI: z}
	workers := []chan expiryOp{m.queue}
	es.workers.Store(&workers)
	prev := globalExpiryState
	globalExpiryState = es
	t.Cleanup(func() { globalExpiryState = prev })
	return m
}

func (m *migrationILM) key(object, versionID string) string {
	if versionID == "" {
		versionID = nullVersionID
	}
	return object + " " + versionID
}

func (m *migrationILM) track(object string) {
	if !slices.Contains(m.objects, object) {
		m.objects = append(m.objects, object)
	}
}

// put writes a version of object to pool 0.
func (m *migrationILM) put(object, label string, opts ObjectOptions) ObjectInfo {
	m.t.Helper()
	oi := putConsistencyObject(m.t, m.z, m.bucket, object, 0, object+" "+label, opts)
	m.labels[m.key(object, oi.VersionID)] = label
	m.track(object)
	return oi
}

// marker writes a delete marker of object to pool 0.
func (m *migrationILM) marker(object, label string, mtime time.Time) {
	m.t.Helper()
	versionID := mustGetUUID()
	_, err := m.z.serverPools[0].DeleteObject(m.t.Context(), m.bucket, object,
		ObjectOptions{Versioned: true, VersionID: versionID, DeleteMarker: true, MTime: mtime})
	if err != nil {
		m.t.Fatal(err)
	}
	m.markers[m.key(object, versionID)] = label
	m.track(object)
}

// tiered writes a version of object to pool 0 whose data is in a remote tier,
// as the remote object "remote" of tier WARM. A zero mtime is now.
func (m *migrationILM) tiered(object, label string, mtime time.Time) ObjectInfo {
	m.t.Helper()
	ctx := m.t.Context()
	oi := m.put(object, label, ObjectOptions{Versioned: true, MTime: mtime})
	infos, errs := readAllFileInfo(ctx, m.z.serverPools[0].getHashedSet(object).getDisks(), "", m.bucket, object, oi.VersionID, false, false)
	if errs[0] != nil {
		m.t.Fatal(errs[0])
	}
	fi := infos[0]
	fi.TransitionStatus, fi.TransitionTier, fi.TransitionedObjName = lifecycle.TransitionComplete, "WARM", "remote"
	if err := m.z.serverPools[0].DecomTieredObject(ctx, m.bucket, object, fi, ObjectOptions{}); err != nil {
		m.t.Fatal(err)
	}
	stored, err := m.z.serverPools[0].GetObjectInfo(ctx, m.bucket, object, ObjectOptions{VersionID: oi.VersionID})
	if err != nil || stored.TransitionedObject.Status != lifecycle.TransitionComplete {
		m.t.Fatalf("fixture is not a transitioned version: %q, %v", stored.TransitionedObject.Status, err)
	}
	return oi
}

func (m *migrationILM) label(object, versionID string, deleted bool) string {
	key := m.key(object, versionID)
	if l, ok := m.markers[key]; ok && deleted {
		return l
	}
	if l, ok := m.labels[key]; ok && !deleted {
		return l
	}
	if deleted {
		return "DM"
	}
	return "?"
}

// move moves the contents of pool 0 out with a decommission, as the admin API
// runs it, including the check for versions left behind, or with a rebalance
// pass, with the walker's accounting and the removal of the source copies.
// The rebalance goal is never reached, so the walk visits every object. The
// test fails unless a decommission completes. ILM has not caught up yet.
func (m *migrationILM) move(kind string) {
	m.t.Helper()
	if kind == "rebalance" {
		migrationTestWalk(m.t, m.z, m.bucket, 0, kind)
		m.z.rebalMu.Lock()
		m.z.rebalMeta.PoolStats[0].Info.Status = rebalCompleted
		m.z.rebalMu.Unlock()
		return
	}
	info := m.decommission()
	if !info.Complete || info.Failed || info.ItemsDecommissionFailed > 0 {
		m.t.Fatalf("decommission did not complete: complete=%t failed=%t itemsFailed=%d; pool 0 holds %v",
			info.Complete, info.Failed, info.ItemsDecommissionFailed, m.view(0))
	}
}

// decommission runs a decommission of pool 0 as the admin API runs it, again
// after a failed one, and returns its outcome.
func (m *migrationILM) decommission() PoolDecommissionInfo {
	m.t.Helper()
	ctx := m.t.Context()
	if err := m.z.StartDecommission(ctx, 0); err != nil {
		m.t.Fatal(err)
	}
	m.z.doDecommissionInRoutine(ctx, 0)
	m.z.poolMetaMutex.RLock()
	defer m.z.poolMetaMutex.RUnlock()
	return *m.z.poolMeta.Pools[0].Decommission
}

// setConfig replaces the bucket's lifecycle rules (none if empty), or sets a
// replication configuration, as the bucket APIs do.
func (m *migrationILM) setConfig(lifecycleRules, replicationRules string) {
	m.t.Helper()
	meta, err := globalBucketMetadataSys.Get(m.bucket)
	if err != nil {
		m.t.Fatal(err)
	}
	meta.LifecycleConfigXML = nil
	if lifecycleRules != "" {
		meta.LifecycleConfigXML = []byte("<LifecycleConfiguration>" + lifecycleRules + "</LifecycleConfiguration>")
	}
	if replicationRules != "" {
		meta.ReplicationConfigXML = []byte("<ReplicationConfiguration>" + replicationRules + "</ReplicationConfiguration>")
	}
	if err := meta.Save(m.t.Context(), m.z); err != nil {
		m.t.Fatal(err)
	}
	globalBucketMetadataSys.Set(m.bucket, meta)
}

// stored returns the metadata of a version of object in a pool.
func (m *migrationILM) stored(pool int, object, versionID string) (FileInfo, error) {
	infos, errs := readAllFileInfo(m.t.Context(), m.z.serverPools[pool].getHashedSet(object).getDisks(), "", m.bucket, object, versionID, false, false)
	return infos[0], errs[0]
}

// freeVersions returns the number of free versions of each object in a pool.
func (m *migrationILM) freeVersions(pool int) map[string]int {
	m.t.Helper()
	free := map[string]int{}
	for _, set := range m.z.serverPools[pool].sets {
		err := set.listObjectsToDecommission(m.t.Context(), decomBucketInfo{Name: m.bucket}, func(entry metaCacheEntry) {
			if fivs, err := entry.fileInfoVersions(m.bucket); err == nil && len(fivs.FreeVersions) > 0 {
				free[entry.name] = len(fivs.FreeVersions)
			}
		})
		if err != nil {
			m.t.Fatal(err)
		}
	}
	return free
}

// migrate moves the contents of pool 0 out and lets ILM catch up.
func (m *migrationILM) migrate(kind string) {
	m.t.Helper()
	m.move(kind)
	m.catchUp()
}

// catchUp lets ILM catch up with data movement out of pool 0: the queued
// expiries run as the expiry workers run them, and the rules are applied to
// the current version of every object as the scanner applies them in the pool
// that holds it now. A transitioned version is not expired: its remote tier is
// not configured here.
func (m *migrationILM) catchUp() {
	m.t.Helper()
	ctx := m.t.Context()
	for len(m.queue) > 0 {
		op := <-m.queue
		task, ok := op.(expiryTask)
		if !ok {
			m.t.Fatalf("unexpected expiry operation %T", op)
		}
		oi := task.objInfo
		m.t.Logf("%s queued %s of %s %s", task.src, task.event.Action, oi.Name, m.label(oi.Name, oi.VersionID, oi.DeleteMarker))
		if oi.TransitionedObject.Status == "" {
			applyExpiryOnNonTransitionedObjects(ctx, m.z, oi, task.event, task.src)
		}
	}

	lc, err := globalLifecycleSys.Get(m.bucket)
	if err != nil {
		return // no rules
	}
	lr, err := globalBucketObjectLockSys.Get(m.bucket)
	if err != nil {
		m.t.Fatal(err)
	}
	for _, object := range m.objects {
		oi, err := m.z.GetObjectInfo(ctx, m.bucket, object, ObjectOptions{})
		if isErrObjectNotFound(err) || isErrMethodNotAllowed(err) {
			continue // deleted
		}
		if err != nil {
			m.t.Fatal(err)
		}
		evt := evalActionFromLifecycle(ctx, *lc, lr, nil, oi)
		if evt.Action.Delete() && oi.TransitionedObject.Status == "" {
			applyExpiryOnNonTransitionedObjects(ctx, m.z, oi, evt, lcEventSrc_Scanner)
		}
	}
}

// latest returns the label of the version a read of object returns.
func (m *migrationILM) latest(object string) string {
	m.t.Helper()
	oi, err := m.z.GetObjectInfo(m.t.Context(), m.bucket, object, ObjectOptions{})
	if err != nil && (!oi.DeleteMarker || !isErrObjectNotFound(err) && !isErrMethodNotAllowed(err)) {
		m.t.Fatal(err)
	}
	return m.label(object, oi.VersionID, oi.DeleteMarker)
}

// view returns the versions of each object in a pool, newest first. Delete
// markers and versions the test did not write are "DM" and "?".
func (m *migrationILM) view(pool int) map[string][]string {
	m.t.Helper()
	view := map[string][]string{}
	for _, set := range m.z.serverPools[pool].sets {
		err := set.listObjectsToDecommission(m.t.Context(), decomBucketInfo{Name: m.bucket}, func(entry metaCacheEntry) {
			fivs, err := entry.fileInfoVersions(m.bucket)
			if err != nil {
				m.t.Errorf("%s: %v", entry.name, err)
				return
			}
			for _, fi := range fivs.Versions {
				view[entry.name] = append(view[entry.name], m.label(entry.name, fi.VersionID, fi.Deleted))
			}
		})
		if err != nil {
			m.t.Fatal(err)
		}
	}
	return view
}

// check fails the test unless pool 0 is empty and pool 1 holds the versions
// of want.
func (m *migrationILM) check(want map[string][]string) {
	m.t.Helper()
	if left := m.view(0); len(left) > 0 {
		m.t.Errorf("pool 0 still holds %v", left)
	}
	got := m.view(1)
	for _, object := range m.objects {
		if !slices.Equal(got[object], want[object]) {
			m.t.Errorf("%s: pool 1 holds %v, want %v", object, got[object], want[object])
		}
	}
}

// versions writes v1, v2 and v3 of each object.
func (m *migrationILM) versions(opts ObjectOptions, objects ...string) {
	for _, object := range objects {
		for _, label := range []string{"v1", "v2", "v3"} {
			m.put(object, label, opts)
		}
	}
}

// aged writes v1, v2 and v3 of each object, five, four and age days ago.
func (m *migrationILM) aged(age int, objects ...string) {
	for _, object := range objects {
		m.put(object, "v1", ObjectOptions{Versioned: true, MTime: UTCNow().AddDate(0, 0, -5)})
		m.put(object, "v2", ObjectOptions{Versioned: true, MTime: UTCNow().AddDate(0, 0, -4)})
		m.put(object, "v3", ObjectOptions{Versioned: true, MTime: UTCNow().AddDate(0, 0, -age)})
	}
}

// migrationILMRule returns a rule for the objects under obj-.
func migrationILMRule(action string) string {
	return "<Rule><ID>rule</ID><Status>Enabled</Status><Filter><Prefix>obj-</Prefix></Filter>" + action + "</Rule>"
}

// migrationILMNoncurrent expires versions that have been noncurrent for a day.
func migrationILMNoncurrent() string {
	return migrationILMRule("<NoncurrentVersionExpiration><NoncurrentDays>1</NoncurrentDays></NoncurrentVersionExpiration>")
}

// migrationILMExpireCurrent expires the current versions at a past date (S3
// expiration dates are midnights).
func migrationILMExpireCurrent() string {
	date := UTCNow().Truncate(24*time.Hour).AddDate(0, 0, -2).Format(time.RFC3339)
	return migrationILMRule("<Expiration><Date>" + date + "</Date></Expiration>")
}

// testMigrationLifecycleExpiry runs the cases that decommission and rebalance
// share.
func testMigrationLifecycleExpiry(t *testing.T, kind string) {
	versioned := ObjectOptions{Versioned: true}

	t.Run("versioned/no-lifecycle", func(t *testing.T) {
		m := newMigrationILM(t, versioning.Enabled, false, "")
		m.versions(versioned, "obj-01", "keep-01")
		m.migrate(kind)
		m.check(map[string][]string{"obj-01": {"v3", "v2", "v1"}, "keep-01": {"v3", "v2", "v1"}})
	})

	// The expiry puts a delete marker on top of v3, which must be moved.
	t.Run("versioned/expire-current", func(t *testing.T) {
		m := newMigrationILM(t, versioning.Enabled, false, migrationILMExpireCurrent())
		m.versions(versioned, "obj-01", "keep-01")
		m.migrate(kind)
		m.check(map[string][]string{"obj-01": {"DM", "v3", "v2", "v1"}, "keep-01": {"v3", "v2", "v1"}})
	})

	// Object lock does not hold back the delete marker, and v3 is under
	// retention.
	t.Run("versioned/expire-current-locked", func(t *testing.T) {
		m := newMigrationILM(t, versioning.Enabled, true, migrationILMExpireCurrent())
		m.put("obj-01", "v1", versioned)
		m.put("obj-01", "v2", versioned)
		v3 := m.put("obj-01", "v3", ObjectOptions{Versioned: true, UserDefined: map[string]string{
			strings.ToLower(xhttp.AmzObjectLockMode):            string(objectlock.RetCompliance),
			strings.ToLower(xhttp.AmzObjectLockRetainUntilDate): amztime.ISO8601Format(UTCNow().Add(24 * time.Hour)),
		}})
		m.migrate(kind)
		m.check(map[string][]string{"obj-01": {"DM", "v3", "v2", "v1"}})
		oi, err := m.z.serverPools[1].GetObjectInfo(t.Context(), m.bucket, "obj-01", ObjectOptions{VersionID: v3.VersionID})
		if mode := objectlock.GetObjectRetentionMeta(oi.UserDefined).Mode; err != nil || mode != objectlock.RetCompliance {
			t.Errorf("v3 retention in pool 1: %q, %v", mode, err)
		}
	})

	// In a suspended bucket the expiry puts a null delete marker on top: a
	// versioned v3 stays, a null current version is replaced.
	t.Run("suspended/expire-current", func(t *testing.T) {
		m := newMigrationILM(t, versioning.Suspended, false, migrationILMExpireCurrent())
		m.versions(versioned, "obj-01")
		m.put("obj-null", "v1", versioned)
		m.put("obj-null", "null", ObjectOptions{VersionSuspended: true})
		m.migrate(kind)
		m.check(map[string][]string{"obj-01": {"DM", "v3", "v2", "v1"}, "obj-null": {"DM", "v1"}})
	})

	// v1 has been noncurrent for four days. The walk has the expiry delete it,
	// queues nothing, and removes the source copies of v2 and v3.
	t.Run("versioned/noncurrent-expiration", func(t *testing.T) {
		m := newMigrationILM(t, versioning.Enabled, false, migrationILMNoncurrent())
		m.aged(0, "obj-01", "keep-01")
		m.move(kind)
		if n := len(m.queue); n != 0 {
			t.Errorf("%d expiries were queued, want none", n)
		}
		m.check(map[string][]string{"obj-01": {"v3", "v2"}, "keep-01": {"v3", "v2", "v1"}})
	})

	// Only a delete marker without noncurrent versions is expired.
	t.Run("versioned/expired-delete-marker", func(t *testing.T) {
		m := newMigrationILM(t, versioning.Enabled, false,
			migrationILMRule("<Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration>"))
		m.marker("obj-dm", "dm", UTCNow())
		m.put("obj-top", "v1", versioned)
		m.marker("obj-top", "dm", UTCNow())
		m.marker("keep-dm", "dm", UTCNow())
		m.migrate(kind)
		m.check(map[string][]string{"obj-top": {"dm", "v1"}, "keep-dm": {"dm"}})
	})

	// ILM deletes every version of obj-01 with v3, and of obj-dm with its
	// marker. Until the queued expiry does, the moved older versions must
	// stay hidden.
	for _, tc := range []struct{ name, rule, object, latest string }{
		{"expire-all-versions", "<Expiration><Days>1</Days><ExpiredObjectAllVersions>true</ExpiredObjectAllVersions></Expiration>", "obj-01", "v3"},
		{"expire-marker-all-versions", "<DelMarkerExpiration><Days>1</Days></DelMarkerExpiration>", "obj-dm", "dm"},
	} {
		t.Run("versioned/"+tc.name, func(t *testing.T) {
			m := newMigrationILM(t, versioning.Enabled, false, migrationILMRule(tc.rule))
			m.aged(3, "obj-01", "keep-01")
			m.put("obj-dm", "v1", ObjectOptions{Versioned: true, MTime: UTCNow().AddDate(0, 0, -5)})
			m.marker("obj-dm", "dm", UTCNow().AddDate(0, 0, -3))
			m.move(kind)
			if got := m.latest(tc.object); got != tc.latest {
				t.Errorf("%s reads as %s before ILM deletes it, want %s; pool 0 holds %v, pool 1 holds %v",
					tc.object, got, tc.latest, m.view(0), m.view(1))
			}
			m.catchUp()
			want := map[string][]string{"obj-01": {"v3", "v2", "v1"}, "keep-01": {"v3", "v2", "v1"}, "obj-dm": {"dm", "v1"}}
			delete(want, tc.object)
			m.check(want)
		})
	}

	t.Run("unversioned/expire-current", func(t *testing.T) {
		m := newMigrationILM(t, "", false, migrationILMExpireCurrent())
		m.put("obj-01", "null", ObjectOptions{})
		m.put("keep-01", "null", ObjectOptions{})
		m.migrate(kind)
		m.check(map[string][]string{"keep-01": {"null"}})
	})
}

func TestPoolsDecommissionLifecycleExpiry(t *testing.T) {
	testMigrationLifecycleExpiry(t, "decommission")

	// A transitioned v3 is moved as metadata; its remote data stays.
	// Rebalance does not move transitioned versions yet.
	t.Run("versioned/expire-current-tiered", func(t *testing.T) {
		m := newMigrationILM(t, versioning.Enabled, false, migrationILMExpireCurrent())
		m.put("obj-01", "v1", ObjectOptions{Versioned: true})
		m.put("obj-01", "v2", ObjectOptions{Versioned: true})
		v3 := m.tiered("obj-01", "v3", time.Time{})
		m.migrate("decommission")
		m.check(map[string][]string{"obj-01": {"v3", "v2", "v1"}})
		oi, err := m.z.serverPools[1].GetObjectInfo(t.Context(), m.bucket, "obj-01", ObjectOptions{VersionID: v3.VersionID})
		if err != nil || oi.TransitionedObject.Status != lifecycle.TransitionComplete {
			t.Errorf("v3 in pool 1 is not transitioned: %q, %v", oi.TransitionedObject.Status, err)
		}
	})
}

// A current version that ILM expires in a versioned bucket stays as a
// noncurrent version. The check after decommission must count it when it is
// left in the pool.
func TestPoolsDecommissionCountsExpiredCurrentVersions(t *testing.T) {
	m := newMigrationILM(t, versioning.Enabled, false, migrationILMExpireCurrent())
	m.migrate("decommission")
	putConsistencyObject(t, m.z, m.bucket, "obj-01", 0, "v1", ObjectOptions{Versioned: true})
	if err := m.z.checkAfterDecom(t.Context(), 0); err == nil {
		t.Fatal("decommission check passed with an expired current version left in the pool")
	}
}

// A version that ILM deletes by itself is deleted by the walk or moved. The
// check after decommission must count one still in the pool, rather than leave
// it to the removal of the pool on the check's own decision, and must not
// delete it either.
func TestPoolsDecommissionCountsVersionsDueForDeletion(t *testing.T) {
	m := newMigrationILM(t, "", false, migrationILMExpireCurrent())
	m.migrate("decommission")
	putConsistencyObject(t, m.z, m.bucket, "obj-01", 0, "null", ObjectOptions{})
	if err := m.z.checkAfterDecom(t.Context(), 0); err == nil {
		t.Fatal("decommission check passed with a version due for deletion left in the pool")
	}
	if _, err := m.stored(0, "obj-01", ""); err != nil {
		t.Errorf("the check deleted the version: %v", err)
	}
	if n := len(m.queue); n != 0 {
		t.Errorf("%d expiries were queued, want none", n)
	}
}

// Rebalance removes the source copies of an object only when it has counted
// all of its versions. A version that ILM deletes must be counted, or the
// moved versions stay in pool 0 as duplicates.
func TestPoolsRebalanceLifecycleExpiry(t *testing.T) {
	testMigrationLifecycleExpiry(t, "rebalance")
}

// versionID returns the version ID of the version of object with label.
func (m *migrationILM) versionID(object, label string) string {
	m.t.Helper()
	for key, l := range m.labels {
		if id, ok := strings.CutPrefix(key, object+" "); ok && l == label {
			return id
		}
	}
	m.t.Fatalf("no version %s of %s", label, object)
	return ""
}

// The walk decides with the lifecycle it loaded when it started on the bucket.
// The deletion applies the rule as it is when it runs: a version whose rule was
// removed or relaxed in between is not deleted, and the walk moves it instead.
func TestExpireMovingVersionRechecksLifecycle(t *testing.T) {
	for _, tc := range []struct{ name, rules string }{
		{"rule-removed", ""},
		{"rule-relaxed", migrationILMRule("<NoncurrentVersionExpiration><NoncurrentDays>30</NoncurrentDays></NoncurrentVersionExpiration>")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newMigrationILM(t, versioning.Enabled, false, migrationILMNoncurrent())
			m.aged(0, "obj-01")
			ctx := t.Context()
			v1, err := m.z.serverPools[0].GetObjectInfo(ctx, m.bucket, "obj-01", ObjectOptions{VersionID: m.versionID("obj-01", "v1")})
			if err != nil {
				t.Fatal(err)
			}
			lc, err := globalLifecycleSys.Get(m.bucket)
			if err != nil {
				t.Fatal(err)
			}
			evt := evalActionFromLifecycle(ctx, *lc, objectlock.Retention{}, nil, v1)
			if evt.Action != lifecycle.DeleteVersionAction {
				t.Fatalf("v1 is not due: %s", evt.Action)
			}

			m.setConfig(tc.rules, "")
			if expireMovingVersion(ctx, m.z, v1, evt, lcEventSrc_Decom) {
				t.Fatal("v1 was deleted on the earlier decision")
			}
			m.migrate("decommission")
			m.check(map[string][]string{"obj-01": {"v3", "v2", "v1"}})
		})
	}
}

// With replication configured, ILM leaves a version alone while its purge waits
// to be replicated, and the walk moves it with its purge state, so replication
// can still complete. A noncurrent version that is due without a pending purge
// is deleted through the expiry path, as the scanner deletes it, and nothing is
// queued.
func TestPoolsMigrationLifecycleExpiryWithReplication(t *testing.T) {
	arn := "arn:minio:replication::" + mustGetUUID() + ":target"
	replicationRules := "<Role></Role><Rule><ID>repl</ID><Status>Enabled</Status><Priority>1</Priority>" +
		"<DeleteMarkerReplication><Status>Enabled</Status></DeleteMarkerReplication>" +
		"<DeleteReplication><Status>Enabled</Status></DeleteReplication>" +
		"<Filter><Prefix></Prefix></Filter><Destination><Bucket>" + arn + "</Bucket></Destination></Rule>"
	for _, kind := range []string{"decommission", "rebalance"} {
		t.Run(kind, func(t *testing.T) {
			m := newMigrationILM(t, versioning.Enabled, false, migrationILMNoncurrent())
			m.setConfig(migrationILMNoncurrent(), replicationRules)
			m.aged(0, "obj-01", "obj-02")
			v1 := m.versionID("obj-02", "v1")
			_, err := m.z.serverPools[0].DeleteObject(t.Context(), m.bucket, "obj-02", ObjectOptions{
				Versioned: true, VersionID: v1,
				DeleteReplication: ReplicationState{
					VersionPurgeStatusInternal: arn + "=PENDING;",
					PurgeTargets:               map[string]VersionPurgeStatusType{arn: replication.VersionPurgePending},
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			m.markers[m.key("obj-02", v1)] = "v1-purge-pending"

			m.move(kind)
			if n := len(m.queue); n != 0 {
				t.Errorf("%d expiries were queued, want none", n)
			}
			m.check(map[string][]string{"obj-01": {"v3", "v2"}, "obj-02": {"v3", "v2", "v1-purge-pending"}})
			if fi, err := m.stored(1, "obj-02", v1); err != nil || fi.VersionPurgeStatus() != replication.VersionPurgePending {
				t.Errorf("v1 of obj-02 in pool 1: purge status %q, %v", fi.VersionPurgeStatus(), err)
			}
		})
	}
}

// migrationILMTier is a remote tier held in memory whose removals can fail.
type migrationILMTier struct {
	tierExpiryTestBackend
	fail atomic.Bool
}

func (b *migrationILMTier) Remove(ctx context.Context, object string, rv remoteVersionID) error {
	if b.fail.Load() {
		return errors.New("tier unavailable")
	}
	return b.tierExpiryTestBackend.Remove(ctx, object, rv)
}

func (b *migrationILMTier) has(object string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.objects[object]
	return ok
}

// A transitioned noncurrent version that ILM deletes during a decommission
// keeps its remote object recorded in the pool until the tier has removed it.
// When the tier fails, the expiry leaves a free version behind, and the
// decommission does not complete. Run again once the tier works, it removes
// the remote object and then the free version. Only the free version on the
// drives carries the remote object from one run to the next, as across a
// restart.
func TestPoolsDecommissionLifecycleExpiryTierFailure(t *testing.T) {
	m := newMigrationILM(t, versioning.Enabled, false, migrationILMNoncurrent())
	tier := &migrationILMTier{tierExpiryTestBackend: tierExpiryTestBackend{objects: map[string][]byte{"remote": []byte("v1")}}}
	tier.fail.Store(true)
	prevTier := globalTierConfigMgr
	globalTierConfigMgr = NewTierConfigMgr()
	globalTierConfigMgr.drivercache["WARM"] = tier
	t.Cleanup(func() { globalTierConfigMgr = prevTier })

	m.tiered("obj-01", "v1", UTCNow().AddDate(0, 0, -5))
	m.put("obj-01", "v2", ObjectOptions{Versioned: true, MTime: UTCNow().AddDate(0, 0, -4)})
	m.put("obj-01", "v3", ObjectOptions{Versioned: true})
	m.versions(ObjectOptions{Versioned: true}, "keep-01")

	if info := m.decommission(); info.Complete {
		t.Fatal("decommission completed while the remote object of v1 was recorded only in pool 0")
	}
	if !tier.has("remote") {
		t.Fatal("the remote object of v1 was removed while the tier failed")
	}
	if got := m.freeVersions(0); got["obj-01"] != 1 {
		t.Fatalf("free versions in pool 0: %v, want one of obj-01", got)
	}
	if n := len(m.queue); n != 0 {
		t.Errorf("%d expiries were queued, want none", n)
	}

	// A decommission start ensures the metadata buckets through the local
	// drives of the node with one write quorum. This node holds both pools,
	// and the first run emptied .minio.sys/buckets in pool 0, so creating it
	// would split evenly between "created" and "exists". Pools on separate
	// nodes do not meet this; recreate the directory in pool 0 for the retry.
	for _, set := range m.z.serverPools[0].sets {
		for _, disk := range set.getDisks() {
			if err := disk.MakeVol(t.Context(), pathJoin(minioMetaBucket, bucketMetaPrefix)); err != nil && !errors.Is(err, errVolumeExists) {
				t.Fatal(err)
			}
		}
	}

	tier.fail.Store(false)
	if info := m.decommission(); !info.Complete || info.Failed {
		t.Fatalf("decommission did not complete once the tier worked: complete=%t failed=%t; free versions in pool 0: %v",
			info.Complete, info.Failed, m.freeVersions(0))
	}
	if tier.has("remote") {
		t.Error("the remote object of v1 was not removed")
	}
	if got := m.freeVersions(0); len(got) > 0 {
		t.Errorf("free versions left in pool 0: %v", got)
	}
	m.check(map[string][]string{"obj-01": {"v3", "v2"}, "keep-01": {"v3", "v2", "v1"}})
}

// Rebalance does not move transitioned versions yet. An object with one stays
// in pool 0 with all its versions, and none of them is copied to pool 1, where
// they would stay as duplicates of the source copies.
func TestPoolsRebalanceLeavesObjectsWithTransitionedVersions(t *testing.T) {
	m := newMigrationILM(t, versioning.Enabled, false, migrationILMNoncurrent())
	m.put("obj-01", "v1", ObjectOptions{Versioned: true, MTime: UTCNow().AddDate(0, 0, -5)})
	m.tiered("obj-01", "v2", UTCNow().AddDate(0, 0, -4))
	m.put("obj-01", "v3", ObjectOptions{Versioned: true})
	m.versions(ObjectOptions{Versioned: true}, "keep-01")

	m.move("rebalance")
	if got := m.view(0); len(got) != 1 || !slices.Equal(got["obj-01"], []string{"v3", "v2", "v1"}) {
		t.Errorf("pool 0 holds %v, want all versions of obj-01 only", got)
	}
	if got := m.view(1); got["obj-01"] != nil || !slices.Equal(got["keep-01"], []string{"v3", "v2", "v1"}) {
		t.Errorf("pool 1 holds %v, want keep-01 only", got)
	}
	if n := len(m.queue); n != 0 {
		t.Errorf("%d expiries were queued, want none", n)
	}
}
