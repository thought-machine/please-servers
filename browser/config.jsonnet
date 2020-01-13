{
    blobstore: {
        content_addressable_storage: {
            grpc: {
                address: "127.0.0.1:7777"
            }
        },
        action_cache: {
            grpc: {
                address: "127.0.0.1:7777"
            }
        }
    },
    listen_address: ":7779",
    maximum_message_size_bytes: 4194304
}