# Using the Airflow Server in AKS

This guide explains how to access the Airflow server deployed in Azure Kubernetes Service (AKS) by port forwarding to the service using either `kubectl` or `k9s`.

> **Pre-requisites**: Ensure you have logged into Azure via `az login` and have the required Kubernetes configuration generated.

---

### Step 1: Log into Azure

Before accessing the Airflow server, you need to log into Azure:

``` bash
az login
```

This command will open a browser window for you to authenticate with your Azure account.

---

### Step 2: Generate kubeconfig for AKS

Once authenticated, use the provided `aks_login.sh` script to generate the Kubernetes configuration:

```bash
source ./scripts/aks_login.sh
```

This script will configure your Kubernetes credentials and save the configuration in the `~/.kube/config` file.

---

### Step 3: Port Forwarding to Airflow

After setting up your Kubernetes configuration, you can access the Airflow server by forwarding the port of the Airflow webserver service:

#### Using `kubectl`

Run the following command to forward port 8080 of the Airflow webserver to your local machine:

```bash
kubectl port-forward svc/airflow-server-api-server 8080:8080
```

#### Using `k9s`

1. Open `k9s` by simply typing:

   ```bash
   k9s
   ```

2. Navigate to the `svc` (services) section and locate the Airflow webserver service by typing `:svc` to filter services.
3. Use the port-forwarding shortcut (`Shift + F`) while selecting the Airflow webserver service.
4. Specify the local port as `8080` when prompted.

---

### Step 4: Access Airflow in Your Browser

Once the port forwarding is active, you can access the Airflow server via your browser at:

```
http://localhost:8080
```

You will now be able to interact with the Airflow web interface.

---

### Notes

- Ensure your Kubernetes context is set to the correct AKS cluster.
- If you encounter any issues, verify that the `aks_login.sh` script completed successfully and that your Kubernetes configuration is up-to-date.
- For troubleshooting, you can inspect logs using `kubectl logs` or `k9s`.

---

Enjoy using your Airflow server!
