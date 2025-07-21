# ABCD2BioSchema

This is a lightweight HTTP API service for submitting and tracking XML transformation jobs via the [GFBio Transformation API](https://transformation.gfbio.org). It supports both file upload and URL-based submission. The service is written in Rust using [Axum](https://github.com/tokio-rs/axum) and provides basic endpoints to trigger transformations and retrieve job metadata.

## Features

- Upload XML files for transformation via multipart form.
- Submit transformations via remote XML URLs.
- Automatically sends requests to GFBio's transformation API.
- Generates job metadata with download references.
- Minimal dependencies and deployable via Docker.

## Endpoints

| Method | Endpoint             | Description                                      |
|--------|----------------------|--------------------------------------------------|
| GET    | `/health`            | Health check                                     |
| POST   | `/transform`         | Submit XML file via multipart upload             |
| POST   | `/transform/url`     | Submit XML via URL                               |

---

## API Usage

### Health Check

```bash
curl http://localhost:3000/health
```

### Transformation via URL

```bash
curl -X POST http://localhost:3000/transform/url \
  -H "Content-Type: application/json" \
  -d '{
    "xml_url": "https://ww3.bgbm.org/tmp/bgbm_herbarium_small.xml",
    "transformation_id": "5"
  }'
```

---

## Running via Docker

```bash
```

A Dockerfile is included to make deployment easy.

```bash
docker build -t abcd2bioschema .
docker run -p 3000:3000 abcd2bioschema
```

Alternatively, you can use a provided shell script to build and run the container:

```bash
chmod +x build_and_run.sh
./build_and_run.sh deploy
```

---

## Configuration
The service uses environment variables for configuration. You can set the variables via a `.env` file or directly in your environment. The following variables are required:
- `GFBIO_BASE_URL`: The base URL for the GFBio Transformation API (currently: `https://transformation.gfbio.org/api`).
- `SERVER_HOST`: The address the server will bind to (default: `0.0.0.0`)
- `SERVER_PORT`: The port the server will listen on (default: `3000`).
- `TEMP_DIR`: Directory for temporary files (default: `/app/tmp`).

---

## Contributing

Contributions to the project are welcome! Contributions to this project are welcome! Whether you find bugs, want to request features, or submit enhancements, please feel free to open an issue or submit a pull request. For major changes, it's recommended to discuss them first to ensure alignment with project goals.
Please read the [`CODE OF CONDUCT`](CODE_OF_CONDUCT.md) to learn more about our guidelines and the contribution process.

---

## License

Licensed under either of

* Apache License, Version 2.0
  ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
* MIT license
  ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option. Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in this Service by you, as defined in the Apache-2.0 license, shall be dually licensed as above, without any additional terms or conditions.

---

## Contact

For inquiries or support regarding this project, you can reach out to the maintainers through GitHub issues.