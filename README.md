# ABCD2BioSchema Service

A webhook-based microservice that transforms ABCD (Access to Biological Collection Data) metadata into BioSchema-compliant JSON format. The service integrates with the [Aruna data orchestration engine](https://aruna-engine.org) and the [GFBio Transformation API](https://transformation.gfbio.org).

## Overview

This service provides automated transformation of biological collection metadata:

- **Input**: ABCD XML files stored in Aruna
- **Process**: Transformation via GFBio API
- **Output**: BioSchema-compliant JSON stored back in Aruna
- **Trigger**: Aruna webhook system

### Features

- Automatic transformation triggered by Aruna hooks
- Support for multipart uploads to S3-compatible storage
- Automatic relationship creation between source and transformed objects
- Metadata preservation and enhancement
- Automatic labeling with transformation status
- Asynchronous processing with status callbacks

### Architecture

```
┌─────────────┐            ┌──────────────────┐         ┌──────────────────┐
│   Aruna     │   webhook  │  ABCD2BioSchema  │  API    │      GFBio       │
│   Storage   ├───────────►│     Service      ├────────►│  Transformation  │
│             │◄───────────┤                  │◄────────┤       API        │
└─────────────┘  callback  └──────────────────┘         └──────────────────┘
   ^                          │
   │                          │
   │     S3 Upload            │
   └──────────────────────────┘
```

### Prerequisites

- Access to an Aruna instance
- Valid API tokens for Aruna

---

## Setting up the Webhook in Aruna

Use the provided [test.sh](test.sh) script to register a webhook in Aruna.

```bash
# Configure the script
TOKEN="your-bearer-token"
PROJECT="your-project-id"
TOKEN_EXPIRATION="unix-timestamp-in-milliseconds"

# Run the script
./test.sh
```

The webhook will trigger when:
- An object is finished uploading
- The object has a label with key `ABCD`

---

## Workflow

1. **Trigger**: User uploads an ABCD XML file to Aruna with label `ABCD`
2. **Webhook**: Aruna triggers the service via POST to `/transform/url`
3. **Download**: Service retrieves the ABCD file from Aruna
4. **Transform**: Service sends XML to GFBio API for transformation
5. **Upload**: Transformed JSON is uploaded back to Aruna as a new object
6. **Relationship**: Service creates an `ORIGIN` relationship linking transformed object to source
7. **Callback**: Services notifies Aruna of completion status
8. **Labels**: The transformed object is labeled with `BioSchema` and `TRANSFORMED_BY_GFBIO`

---

## Created Object Properties

Transformed objects automatically include:

- **Name**: Original filename with `.json` extension
- **Title**: Original title + " BioSchema"
- **Description**: Enhanced description explaining the transformation and the original description of the ABCD file
- **Labels**:
  - `BioSchema: true`
  - `TRANSFORMED_BY_GFBIO: success`
  - And the original labels of the source object (except `ABCD`)
- **Relationships**:
  - `ORIGIN` relationship to the source ABCD file
  - Inherits parent relationship (Project/Collection/Dataset)
- **Licenses**: Inherited from the source object
- **Author**: Inherited from the source object

---

## Error Handling

The service implements comprehensive error handling:

- **Download failures**: Notified via error callback to Aruna
- **Transformation errors**: Logged and reported with error labels
- **Upload failures**: Retry logic with detailed error messages
- **Webhook failures**: Automatic callback with error status

Errors are added as labels to the source object:
```json
{
  "key": "Error",
  "value": "error-message",
  "variant": "LABEL"
}
```
## Logging

The service uses structured logging with tracing:

Log levels:
- `DEBUG`: Detailed operation logs including S3 uploads
- `INFO`: High-level operation status
- `WARN`: Warning conditions and error callbacks
- `ERROR`: Critical failures

---

## Development

### Project Structure

```
.
├── src/
│   ├── main.rs           # Application entry point
│   ├── service.rs        # HTTP endpoint handlers
│   ├── webhook.rs        # Core webhook and transformation logic
│   ├── models.rs         # Data structures and types
│   └── job.rs            # Job status tracking
├── ...              
├── ...           
└── ...             
```

---

## Troubleshooting

### Webhook not triggering
- Verify the hook is registered: Check the list of all of your registered hooks
- Confirm object has `ABCD` label
- Check service logs for incoming requests

### Transformation failures
- Verify GFBio API accessibility
- Check XML file format compliance

### Upload failures
- Verify S3 credentials in the webhook payload
- Check network connectivity to S3 endpoint
- Confirm bucket and permissions

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

## Acknowledgments

- GFBio for the transformation API

---

## Contact

For inquiries or support regarding this project, you can reach out to the maintainers through GitHub issues.