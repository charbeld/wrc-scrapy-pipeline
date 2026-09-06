"""Storage adapters: MongoDB for metadata, S3/MinIO for document bytes."""

from wrc_pipeline.storage.mongo import MongoRepository
from wrc_pipeline.storage.objectstore import ObjectStore

__all__ = ["MongoRepository", "ObjectStore"]
