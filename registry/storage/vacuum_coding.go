package storage

import (
	"context"
	"path"

	dcontext "github.com/juan-chan/distribution/context"
	"github.com/juan-chan/distribution/registry/storage/driver"
	"github.com/opencontainers/go-digest"
)

// vacuum coding contains functions for cleaning up repositories and blobs
// These functions will only reliably work on strongly consistent
// storage systems.
// https://en.wikipedia.org/wiki/Consistency_model

// NewVacuumCoding creates a new VacuumCoding
func NewVacuumCoding(ctx context.Context, driver driver.StorageDriver) VacuumCoding {
	return VacuumCoding{
		ctx:    ctx,
		driver: driver,
	}
}

// VacuumCoding removes content from the filesystem
type VacuumCoding struct {
	driver driver.StorageDriver
	ctx    context.Context
}

// RemoveBlob removes a blob from the filesystem
func (v VacuumCoding) RemoveBlob(host, dgst string) error {
	d, err := digest.Parse(dgst)
	if err != nil {
		return err
	}

	blobPath, err := pathFor(blobPathSpec{digest: d})
	if err != nil {
		return err
	}

	dcontext.GetLogger(v.ctx).Infof("Deleting blob: %s, host: %s", blobPath, host)

	err = v.driver.DeleteWithHost(v.ctx, host, blobPath)
	if err != nil {
		return err
	}

	return nil
}

// RemoveManifest removes a manifest from the filesystem
func (v VacuumCoding) RemoveManifest(host, name string, dgst digest.Digest, tags []string) error {
	// remove a tag manifest reference, in case of not found continue to next one
	for _, tag := range tags {
		tagsPath, err := pathFor(manifestTagIndexEntryPathSpec{name: name, revision: dgst, tag: tag})
		if err != nil {
			return err
		}

		_, err = v.driver.Stat(v.ctx, tagsPath)
		if err != nil {
			switch err := err.(type) {
			case driver.PathNotFoundError:
				continue
			default:
				return err
			}
		}
		dcontext.GetLogger(v.ctx).Infof("deleting manifest tag reference: %s, host: %s", tagsPath, host)
		err = v.driver.DeleteWithHost(v.ctx, host, tagsPath)
		if err != nil {
			return err
		}
	}

	manifestPath, err := pathFor(manifestRevisionPathSpec{name: name, revision: dgst})
	if err != nil {
		return err
	}
	dcontext.GetLogger(v.ctx).Infof("deleting manifest: %s, host: %s", manifestPath, host)
	return v.driver.DeleteWithHost(v.ctx, host, manifestPath)
}

func (v VacuumCoding) BackupAndRemoveBlob(host, dgst string) error {
	d, err := digest.Parse(dgst)
	if err != nil {
		return err
	}

	blobPath, err := pathFor(blobPathSpec{digest: d})
	if err != nil {
		return err
	}

	dcontext.GetLogger(v.ctx).Infof("Deleting blob: %s, host: %s", blobPath, host)

	err = v.driver.BackupAndDeleteWithHost(v.ctx, host, blobPath)
	if err != nil {
		return err
	}

	return nil
}

func (v VacuumCoding) BackupAndRemoveManifest(host, name string, dgst digest.Digest, tags []string) error {
	// remove a tag manifest reference, in case of not found continue to next one
	for _, tag := range tags {
		tagsPath, err := pathFor(manifestTagIndexEntryPathSpec{name: name, revision: dgst, tag: tag})
		if err != nil {
			return err
		}

		_, err = v.driver.Stat(v.ctx, tagsPath)
		if err != nil {
			switch err := err.(type) {
			case driver.PathNotFoundError:
				continue
			default:
				return err
			}
		}
		dcontext.GetLogger(v.ctx).Infof("deleting manifest tag reference: %s, host: %s", tagsPath, host)
		err = v.driver.BackupAndDeleteWithHost(v.ctx, host, tagsPath)
		if err != nil {
			return err
		}
	}

	manifestPath, err := pathFor(manifestRevisionPathSpec{name: name, revision: dgst})
	if err != nil {
		return err
	}
	dcontext.GetLogger(v.ctx).Infof("deleting manifest: %s, host: %s", manifestPath, host)
	return v.driver.BackupAndDeleteWithHost(v.ctx, host, manifestPath)
}

// RemoveRepository removes a repository directory from the
// filesystem
func (v VacuumCoding) RemoveRepository(host, repoName string) error {
	rootForRepository, err := pathFor(repositoriesRootPathSpec{})
	if err != nil {
		return err
	}
	repoDir := path.Join(rootForRepository, repoName)
	dcontext.GetLogger(v.ctx).Infof("Deleting repo: %s, host: %s", repoDir, host)
	err = v.driver.DeleteWithHost(v.ctx, host, repoDir)
	if err != nil {
		return err
	}

	return nil
}
