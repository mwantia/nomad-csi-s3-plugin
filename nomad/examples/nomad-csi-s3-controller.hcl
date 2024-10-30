job "nomad-csi-s3-controller" {
    datacenters = [ "*" ]
    region      = "global"
    node_pool   = "all"
  
    constraint {
        attribute = "${attr.kernel.name}"
        value     = "linux"
    }

    constraint {
        attribute = "${attr.cpu.arch}"
        value     = "amd64"
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
                ]
                privileged = true
            }

            csi_plugin {
                id        = "nomad-csi-s3-plugin"
                type      = "controller"
                mount_dir = "/csi"
            }

            resources {
                cpu    = 100
                memory = 128
            }
        }
    }
}