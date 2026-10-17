// Copyright (c) 2026 Denis Babchuk
//
// This file is part of Silo Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
)

// Report a fixed fill level for allocation only; metadata and data use the
// real fixture disks. 90% is above the reserve that steers new objects to
// other pools but below the hard free drive threshold; 100% is past it.
type placementCapacityDisk struct {
	StorageAPI
	usedPct uint64
}

func (d placementCapacityDisk) DiskInfo(ctx context.Context, opts DiskInfoOptions) (DiskInfo, error) {
	info, err := d.StorageAPI.DiskInfo(ctx, opts)
	info.Total = 1 << 40
	info.Used = info.Total * d.usedPct / 100
	info.Free = info.Total - info.Used
	return info, err
}

func placementCapacity(z *erasureServerPools, object string, usedPct ...uint64) func() {
	var restore []func()
	for i, pool := range z.serverPools {
		restore = append(restore, conditionalPutSwapDisks(pool, object, func(disk StorageAPI) StorageAPI {
			return placementCapacityDisk{StorageAPI: disk, usedPct: usedPct[i]}
		}))
	}
	return func() {
		for _, fn := range restore {
			fn()
		}
	}
}

// Capacity with the object's pool at pct and the other pool empty.
func placementHomeCapacity(z *erasureServerPools, object string, home int, pct uint64) func() {
	used := make([]uint64, len(z.serverPools))
	used[home] = pct
	return placementCapacity(z, object, used...)
}

// placementPools returns the pools holding versionID of object, delete
// markers included. An empty versionID addresses the latest version.
func placementPools(t *testing.T, z *erasureServerPools, bucket, object, versionID string) []int {
	t.Helper()
	var pools []int
	for i, pool := range z.serverPools {
		oi, err := pool.GetObjectInfo(t.Context(), bucket, object, ObjectOptions{VersionID: versionID})
		switch {
		case err == nil, oi.DeleteMarker:
			pools = append(pools, i)
		case !isErrObjectNotFound(err) && !isErrVersionNotFound(err):
			t.Fatalf("pool %d: %v", i, err)
		}
	}
	return pools
}

func placementPut(t *testing.T, z *erasureServerPools, bucket, object, body string, opts ObjectOptions) ObjectInfo {
	t.Helper()
	oi, err := z.PutObject(t.Context(), bucket, object,
		mustGetPutObjReader(t, bytes.NewBufferString(body), int64(len(body)), "", ""), opts)
	if err != nil {
		t.Fatal(err)
	}
	return oi
}

func TestPoolsOverwriteStaysInObjectPool(t *testing.T) {
	for _, mode := range []string{"unversioned", "versioned", "suspended"} {
		t.Run(mode, func(t *testing.T) {
			z, bucket := consistencyPools(t)
			opts := ObjectOptions{Versioned: mode == "versioned", VersionSuspended: mode == "suspended"}
			for home := range z.serverPools {
				object := fmt.Sprintf("object-%d", home)
				putConsistencyObject(t, z, bucket, object, home, "v0", opts)
				// Capacity-weighted selection would always move the object to the
				// other pool: only that pool is below the reserve.
				defer placementHomeCapacity(z, object, home, 90)()
				var versions []string
				for i := 1; i <= 5; i++ {
					versions = append(versions, placementPut(t, z, bucket, object, fmt.Sprintf("v%d", i), opts).VersionID)
				}
				if got := placementPools(t, z, bucket, object, ""); !slices.Equal(got, []int{home}) {
					t.Errorf("%s was in pool %d, after overwrites it is in pools %v", object, home, got)
					continue
				}
				if mode != "versioned" {
					continue
				}
				for _, versionID := range versions {
					if got := placementPools(t, z, bucket, object, versionID); !slices.Equal(got, []int{home}) {
						t.Errorf("%s version %s is in pools %v, want [%d]", object, versionID, got, home)
					}
				}
			}
		})
	}
}

// Without room for the write in the object's pool an overwrite still goes to
// another pool, as before, and fails only when no pool has room.
func TestPoolsOverwriteFullObjectPool(t *testing.T) {
	z, bucket := consistencyPools(t)
	const object = "full"
	opts := ObjectOptions{Versioned: true}
	putConsistencyObject(t, z, bucket, object, 0, "v0", opts)

	restore := placementHomeCapacity(z, object, 0, 100)
	oi := placementPut(t, z, bucket, object, "v1", opts)
	restore()
	if got := placementPools(t, z, bucket, object, oi.VersionID); !slices.Equal(got, []int{1}) {
		t.Fatalf("overwrite with pool 0 full went to pools %v, want [1]", got)
	}

	defer placementCapacity(z, object, 100, 100)()
	_, err := z.PutObject(t.Context(), bucket, object,
		mustGetPutObjReader(t, bytes.NewBufferString("v2"), 2, "", ""), opts)
	if !errors.As(err, &StorageFull{}) {
		t.Fatalf("overwrite with all pools full: got %v, want StorageFull", err)
	}
}

// A single pool keeps the free drive threshold from #21115 for overwrites.
func TestSinglePoolOverwriteFreeDriveThreshold(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	obj, dirs, err := prepareErasure16(ctx)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	z := obj.(*erasureServerPools)
	t.Cleanup(func() { cancel(); z.Shutdown(context.Background()); removeRoots(dirs) })
	bucket := "single-pool-placement"
	if err := z.MakeBucket(t.Context(), bucket, MakeBucketOptions{}); err != nil {
		t.Fatal(err)
	}
	const object = "threshold"
	placementPut(t, z, bucket, object, "v0", ObjectOptions{})

	restore := placementCapacity(z, object, 90)
	placementPut(t, z, bucket, object, "v1", ObjectOptions{})
	restore()

	defer placementCapacity(z, object, 100)()
	_, err = z.PutObject(t.Context(), bucket, object,
		mustGetPutObjReader(t, bytes.NewBufferString("v2"), 2, "", ""), ObjectOptions{})
	if !errors.As(err, &StorageFull{}) {
		t.Fatalf("overwrite with the pool full: got %v, want StorageFull", err)
	}
}

// A draining pool does not take new versions, even of objects it holds.
func TestPoolsOverwriteSkipsDrainingPool(t *testing.T) {
	for _, rebalance := range []bool{false, true} {
		t.Run(fmt.Sprintf("rebalance=%t", rebalance), func(t *testing.T) {
			z, bucket := consistencyPools(t)
			const object = "draining"
			opts := ObjectOptions{Versioned: true}
			putConsistencyObject(t, z, bucket, object, 0, "v0", opts)
			if rebalance {
				z.rebalMu.Lock()
				z.rebalMeta = &rebalanceMeta{PoolStats: []*rebalanceStats{
					{Participating: true, Info: rebalanceInfo{Status: rebalStarted}}, {},
				}}
				z.rebalMu.Unlock()
			} else {
				z.poolMetaMutex.Lock()
				z.poolMeta.Pools[0].Decommission = &PoolDecommissionInfo{}
				z.poolMetaMutex.Unlock()
			}
			defer placementHomeCapacity(z, object, 1, 90)()
			oi := placementPut(t, z, bucket, object, "v1", opts)
			if got := placementPools(t, z, bucket, object, oi.VersionID); !slices.Equal(got, []int{1}) {
				t.Fatalf("overwrite of an object in a draining pool went to pools %v, want [1]", got)
			}
		})
	}
}

// Version history behind a latest delete marker stays in the marker's pool.
func TestPoolsOverwriteAfterDeleteMarker(t *testing.T) {
	z, bucket := consistencyPools(t)
	opts := ObjectOptions{Versioned: true}
	for home := range z.serverPools {
		object := fmt.Sprintf("marker-%d", home)
		putConsistencyObject(t, z, bucket, object, home, "v0", opts)
		defer placementHomeCapacity(z, object, home, 90)()
		dm, err := z.DeleteObject(t.Context(), bucket, object, opts)
		if err != nil {
			t.Fatal(err)
		}
		oi := placementPut(t, z, bucket, object, "v1", opts)
		for _, versionID := range []string{dm.VersionID, oi.VersionID} {
			if got := placementPools(t, z, bucket, object, versionID); !slices.Equal(got, []int{home}) {
				t.Errorf("%s version %s is in pools %v, want [%d]", object, versionID, got, home)
			}
		}
	}
}

func TestPoolsMultipartOverwriteStaysInObjectPool(t *testing.T) {
	z, bucket := consistencyPools(t)
	opts := ObjectOptions{Versioned: true}
	for home := range z.serverPools {
		object := fmt.Sprintf("multipart-%d", home)
		putConsistencyObject(t, z, bucket, object, home, "v0", opts)
		defer placementHomeCapacity(z, object, home, 90)()
		mp, err := z.NewMultipartUpload(t.Context(), bucket, object, opts)
		if err != nil {
			t.Fatal(err)
		}
		part, err := z.PutObjectPart(t.Context(), bucket, object, mp.UploadID, 1,
			mustGetPutObjReader(t, bytes.NewBufferString("v1"), 2, "", ""), opts)
		if err != nil {
			t.Fatal(err)
		}
		oi, err := z.CompleteMultipartUpload(t.Context(), bucket, object, mp.UploadID,
			[]CompletePart{{PartNumber: 1, ETag: part.ETag}}, opts)
		if err != nil {
			t.Fatal(err)
		}
		if got := placementPools(t, z, bucket, object, oi.VersionID); !slices.Equal(got, []int{home}) {
			t.Errorf("%s multipart version is in pools %v, want [%d]", object, got, home)
		}
	}
}
