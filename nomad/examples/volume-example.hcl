plugin_id = "nomad-csi-s3-plugin"
type      = "csi"
id        = "volume-example"
name      = "volume-example"

capacity_min = "1GiB"
capacity_max = "10GiB"

capability { 
    access_mode     = "single-node-writer"
    attachment_mode = "file-system"
}

secrets {
    accessKeyID     = "minioadmin"
    secretAccessKey = "minioadmin"
    endpoint        = "http://127.0.0.1:9000"
}

parameters {
    mounter = "s3fs"
}