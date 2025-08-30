# Zabbix Data Replicator

This application replicates Zabbix host data and history between two Zabbix instances.

## Setup

1.  **Clone the repository:**
    ```bash
    git clone <repository_url>
    cd zabbix_data_replicator
    ```

2.  **Create a virtual environment:**
    ```bash
    python3 -m venv .venv
    ```

3.  **Activate the virtual environment:**
    ```bash
    source .venv/bin/activate
    ```

4.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

5.  **Configure environment variables:**
    Create a `.env` file in the project root directory with your Zabbix connection details:
    ```dotenv
    SOURCE_ZABBIX_URL=https://your-source-zabbix.com/api_jsonrpc.php
    SOURCE_ZABBIX_TOKEN=your_source_api_token
    DEST_ZABBIX_URL=https://your-dest-zabbix.com/api_jsonrpc.php
    DEST_ZABBIX_TOKEN=your_dest_api_token
    DEST_TRAPPER_HOST=your_dest_zabbix_server_ip_or_hostname # Host where Zabbix Server/Proxy receives traps
    DEST_TRAPPER_PORT=10051 # Default Zabbix trapper port
    ```

## Running the Application

### Development Mode (Manual Start)

```bash
source .venv/bin/activate
flask run --host=0.0.0.0 --port=5000
```
*Note: `use_reloader=False` is set in `app.py` to prevent the scheduler running twice in debug mode.*

### Production Mode (Systemd Service)

To run the application reliably in the background and ensure it restarts automatically after reboots or crashes, set it up as a systemd service:

1.  **Create the service file:**
    Create a file named `zabbix_replicator.service` in `/etc/systemd/system/` using `sudo`:
    ```bash
    sudo nano /etc/systemd/system/zabbix_replicator.service
    ```
    Paste the following content into the file. **Ensure the `User`, `Group`, and paths in `WorkingDirectory`, `Environment`, and `ExecStart` match your system setup.**

    ```systemd
    [Unit]
    Description=Zabbix Data Replicator Service
    After=network.target

    [Service]
    User=mitssadmin  # CHANGE THIS if needed
    Group=mitssadmin # CHANGE THIS if needed
    WorkingDirectory=/home/mitssadmin/code/zabbix_data_replicator # CHANGE THIS to your project path
    Environment="PATH=/home/mitssadmin/code/zabbix_data_replicator/.venv/bin" # CHANGE THIS to your project path
    ExecStart=/home/mitssadmin/code/zabbix_data_replicator/.venv/bin/gunicorn --workers 3 --bind 0.0.0.0:5000 app:app # CHANGE THIS to your project path
    Restart=always

    [Install]
    WantedBy=multi-user.target
    ```
    Save and close the file (Ctrl+X, then Y, then Enter in `nano`).

2.  **Reload systemd, enable and start the service:**
    ```bash
    sudo systemctl daemon-reload
    sudo systemctl enable zabbix_replicator.service
    sudo systemctl start zabbix_replicator.service
    ```

3.  **Check the service status:**
    ```bash
    sudo systemctl status zabbix_replicator.service
    ```

4.  **(Optional) View logs:**
    ```bash
    sudo journalctl -u zabbix_replicator.service -f
    ```

## Usage

Access the web interface by navigating to `http://<your_server_ip>:5000` in your browser.

1.  The application will attempt to load configuration from the `.env` file.
2.  If needed, you can override or provide missing details via the web interface configuration section.
3.  Select a source host from the dropdown list.
4.  Click "Replicate Host".
5.  The application will:
    *   Fetch the source host configuration (including items, templates, groups, macros).
    *   Map source groups/templates to destination entities by name (creating if necessary).
    *   Create the host on the destination if it doesn't exist.
    *   Modify direct host items to be "Zabbix Trapper" type on the destination.
    *   Create standard ICMP, Interface Link Down/Speed Change, CPU, and Temperature triggers on the destination host.
    *   Fetch the last 24 hours of history data for the source host's items.
    *   Schedule a background job to periodically fetch new data from the source and send it to the destination trapper items.
6.  Monitor the replication status in the "Replication Status" section.
