# Dataflow workers run without external IPs (the org denies compute.vmExternalIpAccess).
# A dedicated subnet with Private Google Access lets them reach Pub/Sub, BigQuery, GCS
# and the SDK container registry; no Cloud NAT is needed because the pipeline has no
# PyPI dependencies beyond the Beam SDK image.
resource "google_compute_network" "dataflow" {
  name                    = "dataflow-vpc"
  auto_create_subnetworks = false
  depends_on              = [google_project_service.apis]
}

resource "google_compute_subnetwork" "dataflow" {
  name                     = "dataflow"
  region                   = var.region
  network                  = google_compute_network.dataflow.id
  ip_cidr_range            = "10.50.0.0/20"
  private_ip_google_access = true
}

# Workers talk to each other for shuffle/streaming on 12345-12346.
resource "google_compute_firewall" "dataflow_internal" {
  name    = "dataflow-internal"
  network = google_compute_network.dataflow.name

  allow {
    protocol = "tcp"
    ports    = ["12345-12346"]
  }
  source_ranges = [google_compute_subnetwork.dataflow.ip_cidr_range]
  target_tags   = ["dataflow"]
}
