job "nomad-csi-s3-node" {
    datacenters = [ "*" ]
    region      = "global"
    node_pool   = "all"
    type        = "system"
  
    constraint {
        attribute = "${attr.kernel.name}"
        value     = "linux"
    }

    group "controllers" {
        count = 1

        ephemeral_disk {
            size = 150
        }

        task "plugin" {
            driver = "docker"

            config {
                image      = "mwantia/nomad-csi-s3-plugin:latest"
                args       = [
                    "--endpoint=unix://csi/csi.sock", 
                    "--nodeid=${node.unique.name}", 
                    "--config=/secrets/config.yml"
                ]
                privileged = true
            }

            csi_plugin {
                id        = "nomad-csi-s3-plugin"
                type      = "node"
                mount_dir = "/csi"
            }

            template {
                data        = <<-EOH
                aliases:
                  - name: minio
                    endpoint: http://minio:9000
                    accessKeyID: minioadmin
                    secretAccessKey: minioadmin
                EOH
                change_mode = "noop"
                destination = "secrets/config.yml"
            }

            resources {
                cpu    = 100
                memory = 128
            }
        }
    }
}