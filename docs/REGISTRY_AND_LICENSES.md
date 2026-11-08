# Registry selection and dependency notices

The development stack selects Apicurio Registry 2.6.8.Final. Its tagged source uses
[Apache License 2.0](https://github.com/Apicurio/apicurio-registry/blob/2.6.8.Final/LICENSE).
The configured in-memory registry loses artifacts on restart; durable schema storage
and cross-language compatibility tests remain future implementation work.

Kafka uses Apache License 2.0. PostgreSQL uses the PostgreSQL License. MinIO server
uses AGPL-3.0-or-later; review the upstream license and applicable obligations before
distributing or operating a modified service. Java and Python dependencies retain
their respective licenses. This note does not assign a license to the project.

## MinIO development build

[The checked-in Dockerfile](../deploy/Dockerfile.minio) builds unmodified upstream
MinIO source rather than relying on an unavailable binary image tag.

- Release: RELEASE.2025-04-22T22-12-26Z.
- Official source commit: 0d7408fc9969caf07de6a8c3a84f9fbb10a6739e.
- [Pinned source archive](https://codeload.github.com/minio/minio/legacy.tar.gz/0d7408fc9969caf07de6a8c3a84f9fbb10a6739e).
- Archive SHA-256: dccf285038a63d48ad52efdfa38c92af525f2d44b0217df5fdc9dc83d7d6075d.
- Go 1.24.2 builder; read-only upstream module graph.
- Explicit DEVELOPMENT build identity; upstream license retained in the image.
- Runtime UID/GID 10001.

This build is not a vendor-supplied binary, a vendor-support claim, or approval for
production use. Check lifecycle, availability, licensing and security before deployment.

[MinIO source and license](https://github.com/minio/minio/tree/0d7408fc9969caf07de6a8c3a84f9fbb10a6739e)
