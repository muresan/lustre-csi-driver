# Helm Charts

This directory contains charts to deploy the lustre CSI driver with Helm.
This deploys the [CSIDriver](v0.0.5/lustre-csi-driver/templates/driver.yaml),
[DaemonSet](v0.0.5/lustre-csi-driver/templates/plugin.yaml), [Namespace](v0.0.5/lustre-csi-driver/templates/namespace.yaml),
and [ServiceAccount](v0.0.5/lustre-csi-driver/templates/rbac.yaml) Kubernetes resources. With `failover.enabled=true`,
it also deploys the RBAC that the node plugin needs for failover: reading PVs, and writing its health Lease and Events.
See "Failover" in the [project README](../README.md#failover).

## To deploy from this workarea:

- To deploy directly from this workarea: `cd charts/ && helm install lustre-csi-driver v0.0.5/lustre-csi-driver/ --values v0.0.5/lustre-csi-driver/values.yaml`
- To enable failover, add `--set failover.enabled=true`
- To shut down: `helm delete lustre-csi-driver`

### To deploy from a github repo

NOTE: The master branch places the charts in the charts/ directory. The release branch will own the chart-releases/ directory.

After merging your changes into the master branch, run the following on your local machine:

```console
helm repo add lustre-csi-driver https://raw.githubusercontent.com/HewlettPackard/lustre-csi-driver/master/charts
```

To use the release branch:

```console
helm repo add lustre-csi-driver https://raw.githubusercontent.com/HewlettPackard/lustre-csi-driver/releases/v0/chart-releases
```

Then search for chart and install it:

```console
helm search repo lustre-csi-driver
helm install lustre-csi-driver lustre-csi-driver/lustre-csi-driver
```
