// Frontend JavaScript for Zabbix Data Replicator

document.addEventListener('DOMContentLoaded', async () => { // Made the event listener async
    console.log("DOM fully loaded and parsed");

    // --- DOM Elements ---
    const configForm = document.getElementById('config-form');
    const connectButton = document.getElementById('connect-button');
    const configStatus = document.getElementById('config-status');

    const hostSelectionSection = document.getElementById('host-selection-section');
    const hostSelect = document.getElementById('host-select');
    const replicateButton = document.getElementById('replicate-button');
    const replicationStartStatus = document.getElementById('replication-start-status');

    const statusSection = document.getElementById('status-section');
    const replicationStatusList = document.getElementById('replication-status-list');
    const clearCompletedBtn = document.getElementById('clear-completed-btn');
    const hostFilterInput = document.getElementById('host-filter-input');

    // Orphaned Host Management Elements
    const checkOrphansButton = document.getElementById('check-orphans-button');
    const orphansStatus = document.getElementById('orphans-status');
    const orphanedHostsList = document.getElementById('orphaned-hosts-list');
    const relinkFormContainer = document.getElementById('relink-form-container');
    const relinkDestHostId = document.getElementById('relink-dest-host-id');
    const relinkDestHostName = document.getElementById('relink-dest-host-name');
    const relinkSourceHostId = document.getElementById('relink-source-host-id');
    const relinkButton = document.getElementById('relink-button');
    const relinkStatus = document.getElementById('relink-status');


    // --- Event Listeners ---

    // Host Filter Input
    hostFilterInput.addEventListener('input', () => {
        filterByHost(hostFilterInput.value);
    });

    // Clear Completed Button
    clearCompletedBtn.addEventListener('click', () => {
        clearCompletedStatuses();
    });

    // Connect Button Click
    connectButton.addEventListener('click', async () => {
        console.log("Connect button clicked");
        configStatus.textContent = 'Connecting...';
        connectButton.disabled = true;
        hostSelect.innerHTML = '<option value="">-- Loading Hosts --</option>'; // Reset dropdown
        hostSelectionSection.style.display = 'none'; // Hide host selection initially
        replicateButton.disabled = true;

        const formData = new FormData(configForm);
        const configData = Object.fromEntries(formData.entries());

        try {
            // Call /api/config
            const configResponse = await fetch('/api/config', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(configData),
            });

            if (!configResponse.ok) {
                const errorData = await configResponse.json();
                throw new Error(errorData.error || `HTTP error! status: ${configResponse.status}`);
            }
            console.log("Config saved successfully via API.");
            configStatus.textContent = 'Configuration saved. Loading hosts...';

            // Call /api/source/hosts
            const hostsResponse = await fetch('/api/source/hosts');
            if (!hostsResponse.ok) {
                const errorData = await hostsResponse.json();
                throw new Error(errorData.error || `HTTP error! status: ${hostsResponse.status}`);
            }
            const hosts = await hostsResponse.json();
            console.log("Hosts fetched successfully via API:", hosts);

            // Populate host dropdown
            hostSelect.innerHTML = '<option value="">-- Select a Host --</option>'; // Clear loading message
            if (hosts && hosts.length > 0) {
                hosts.forEach(host => {
                    const option = document.createElement('option');
                    option.value = host.hostid;
                    // Display name and primary interface if available (simple example)
                    let displayName = host.name;
                    if (host.interfaces && host.interfaces.length > 0) {
                        // Find the first non-agent interface if possible, otherwise first agent
                        let primaryInterface = host.interfaces.find(iface => iface.type !== '1') || host.interfaces[0];
                        if (primaryInterface) {
                            displayName += ` (${primaryInterface.ip || primaryInterface.dns || 'N/A'})`;
                        }
                    }
                    option.textContent = displayName;
                    hostSelect.appendChild(option);
                });
            } else {
                hostSelect.innerHTML = '<option value="">-- No hosts found --</option>';
            }


            configStatus.textContent = 'Connected. Hosts loaded.';
            hostSelectionSection.style.display = 'block'; // Show host selection
            document.getElementById('orphaned-hosts-section').style.display = 'block'; // Show orphaned hosts section
            checkOrphansButton.disabled = false; // Enable check orphans button
            connectButton.disabled = false; // Re-enable connect button

        } catch (error) {
            console.error("Error during connection/host loading:", error);
            configStatus.textContent = `Error: ${error.message || 'Failed to connect or load hosts.'}`;
            connectButton.disabled = false;
            document.getElementById('orphaned-hosts-section').style.display = 'none'; // Hide orphaned hosts section on error
            checkOrphansButton.disabled = true; // Disable check orphans button on error
        }
    });


    // Host Selection Change
    hostSelect.addEventListener('change', () => {
        if (hostSelect.value) {
            replicateButton.disabled = false;
        } else {
            replicateButton.disabled = true;
        }
    });

    // Replicate Button Click
    replicateButton.addEventListener('click', async () => {
        const selectedHostId = hostSelect.value;
        const selectedHostName = hostSelect.options[hostSelect.selectedIndex].text;
        if (!selectedHostId) {
            replicationStartStatus.textContent = 'Please select a host.';
            return;
        }

        console.log(`Replicate button clicked for host ID: ${selectedHostId} (${selectedHostName})`);
        replicationStartStatus.textContent = `Starting replication for ${selectedHostName}...`;
        replicateButton.disabled = true;
        hostSelect.disabled = true; // Disable selection during replication start

        try {
            // Call /api/replicate
            const replicateResponse = await fetch('/api/replicate', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ hostid: selectedHostId }),
            });

            const replicateData = await replicateResponse.json();

            if (!replicateResponse.ok) {
                // Handle cases like 202 Accepted (already running) separately if needed
                if (replicateResponse.status === 202) {
                    replicationStartStatus.textContent = replicateData.message || `Replication already running for ${selectedHostName}.`;
                } else {
                    throw new Error(replicateData.error || `HTTP error! status: ${replicateResponse.status}`);
                }
            } else {
                replicationStartStatus.textContent = replicateData.message || `Replication initiated for ${selectedHostName}. Check status below.`;
            }

            // Add to status list immediately (or update if exists)
            updateReplicationStatus(selectedHostId, selectedHostName, 'Initiated. Waiting for status update...');

            // Start polling if not already started
            startStatusPolling();

        } catch (error) {
            console.error("Error starting replication:", error);
            replicationStartStatus.textContent = `Error: ${error.message || 'Failed to start replication.'}`;
        } finally {
            // Keep replicate button disabled until status shows completion/failure? Or re-enable?
            // For simplicity now, re-enable after attempt.
            replicateButton.disabled = false;
            hostSelect.disabled = false;
        }
    });


    // --- Helper Functions ---
    let pollingIntervalId = null;
    const activePollingHosts = new Set(); // Track hosts we are actively polling for

    // Host filtering function
    function filterByHost(searchTerm) {
        const statusItems = replicationStatusList.querySelectorAll('.status-item');
        const term = searchTerm.toLowerCase().trim();

        if (!term) {
            // Show all items if search is empty
            statusItems.forEach(item => {
                item.style.display = 'flex';
            });
            return;
        }

        statusItems.forEach(item => {
            const hostName = item.dataset.hostName.toLowerCase();
            const hostId = item.dataset.hostId.toLowerCase();
            const shouldShow = hostName.includes(term) || hostId.includes(term);
            item.style.display = shouldShow ? 'flex' : 'none';
        });
    }

    function clearCompletedStatuses() {
        const completedItems = replicationStatusList.querySelectorAll('.status-item[data-status="completed"]');
        completedItems.forEach(item => {
            item.remove();
        });

        // Show empty state if no items left
        if (replicationStatusList.children.length === 1 && replicationStatusList.querySelector('.status-empty')) {
            // Only empty state remains, good
        } else if (replicationStatusList.children.length === 0) {
            showEmptyState();
        }
    }

    function showEmptyState() {
        replicationStatusList.innerHTML = `
            <div class="status-empty">
                <i class="fas fa-info-circle"></i>
                <p>No replication tasks yet. Start by selecting a host to replicate.</p>
            </div>
        `;
    }

    function getStatusIcon(status) {
        const icons = {
            pending: 'fas fa-clock',
            running: 'fas fa-play-circle',
            completed: 'fas fa-check-circle',
            failed: 'fas fa-times-circle',
            error: 'fas fa-exclamation-triangle'
        };
        return icons[status] || 'fas fa-question-circle';
    }

    function getStatusColor(status) {
        const colors = {
            pending: 'pending',
            running: 'running',
            completed: 'completed',
            failed: 'failed',
            error: 'error'
        };
        return colors[status] || 'pending';
    }

    function updateReplicationStatus(hostId, hostName, statusText, statusObj = null) {
        // Remove empty state if it exists
        const emptyState = replicationStatusList.querySelector('.status-empty');
        if (emptyState) {
            emptyState.remove();
        }

        let statusItem = document.getElementById(`status-${hostId}`);
        let isNew = false;

        if (!statusItem) {
            isNew = true;
            statusItem = document.createElement('div');
            statusItem.id = `status-${hostId}`;
            statusItem.className = 'status-item';
            statusItem.dataset.hostId = hostId;
            statusItem.dataset.hostName = hostName;
            replicationStatusList.appendChild(statusItem);
            activePollingHosts.add(hostId);
            startStatusPolling();
        }

        // Determine status type from status text or object
        let statusType = 'pending';
        if (statusObj && statusObj.status) {
            statusType = statusObj.status.toLowerCase();
        } else {
            // Try to infer from status text
            const text = statusText.toLowerCase();
            if (text.includes('completed') || text.includes('success')) {
                statusType = 'completed';
            } else if (text.includes('failed') || text.includes('error')) {
                statusType = 'failed';
            } else if (text.includes('running') || text.includes('processing')) {
                statusType = 'running';
            } else if (text.includes('initiated') || text.includes('waiting')) {
                statusType = 'pending';
            }
        }

        statusItem.dataset.status = statusType;

        // Format timestamp
        const timestamp = new Date().toLocaleString();
        const shortTime = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });

        // Create the enhanced status item HTML
statusItem.innerHTML = `
  <div class="status-content">
    <div class="status-header">
      <span class="status-host">
        <i class="fas fa-server"></i> ${hostName} <small>(ID: ${hostId})</small>
      </span>
      <span class="status-time" title="${timestamp}">${shortTime}</span>
      ${statusType === 'completed' ? `
        <button class="status-action-btn" onclick="this.closest('.status-item').remove();" title="Dismiss">
          <i class="fas fa-times"></i>
        </button>` : ''}
    </div>
    <div class="status-message">${statusText}</div>
    ${statusType === 'running' ? `<div class="status-progress indeterminate"></div>` : ''}
  </div>
`;

        // Apply current host filter if there's a search term
        const currentSearchTerm = hostFilterInput.value;
        if (currentSearchTerm.trim()) {
            filterByHost(currentSearchTerm);
        }

        // Add animation for new items
        if (isNew) {
            statusItem.style.opacity = '0';
            statusItem.style.transform = 'translateY(-10px)';
            setTimeout(() => {
                statusItem.style.transition = 'all 0.3s ease';
                statusItem.style.opacity = '1';
                statusItem.style.transform = 'translateY(0)';
            }, 10);
        }
    }

    async function pollStatus() {
        if (activePollingHosts.size === 0) {
            console.log("Polling: No active hosts to poll. Stopping.");
            stopStatusPolling();
            return;
        }
        console.log("Polling: Fetching status for hosts:", Array.from(activePollingHosts));
        try {
            const response = await fetch('/api/replay/status');
            if (!response.ok) {
                console.error(`Polling: HTTP error! status: ${response.status}`);
                // Maybe stop polling on server error? Or just log and continue?
                return;
            }
            const allStatuses = await response.json();

            // Update status for each active host
            activePollingHosts.forEach(hostId => {
                const taskStatus = allStatuses[hostId];
                const statusDiv = document.getElementById(`status-${hostId}`);
                const hostName = statusDiv ? statusDiv.dataset.hostName : `Host ${hostId}`; // Get stored name

                if (taskStatus && statusDiv) {
                    updateReplicationStatus(hostId, hostName, taskStatus.message || taskStatus.status || 'Unknown status', taskStatus);

                    // If task is completed or failed, remove from active polling
                    if (taskStatus.status === 'completed' || taskStatus.status === 'failed') {
                        console.log(`Polling: Host ${hostId} finished (${taskStatus.status}). Removing from active polling.`);
                        activePollingHosts.delete(hostId);
                    }
                } else if (!taskStatus && statusDiv) {
                    // Task disappeared from backend? Maybe completed long ago or error?
                    console.log(`Polling: Status for host ${hostId} not found in response. Removing from active polling.`);
                    updateReplicationStatus(hostId, hostName, 'Status unavailable (possibly finished or error).');
                    activePollingHosts.delete(hostId);
                }
            });

            // If no hosts left to poll, stop the interval
            if (activePollingHosts.size === 0) {
                stopStatusPolling();
            }

        } catch (error) {
            console.error("Polling: Error fetching status:", error);
            // Decide whether to stop polling on fetch error
        }
    }

    function startStatusPolling() {
        if (!pollingIntervalId && activePollingHosts.size > 0) {
            console.log("Starting status polling.");
            // Poll immediately first time
            pollStatus();
            // Then set interval
            pollingIntervalId = setInterval(pollStatus, 5000); // Poll every 5 seconds
        }
    }

    function stopStatusPolling() {
        if (pollingIntervalId) {
            console.log("Stopping status polling.");
            clearInterval(pollingIntervalId);
            pollingIntervalId = null;
        }
    }

    // --- Event Listeners for Orphaned Host Management ---
    checkOrphansButton.addEventListener('click', async () => {
        console.log("Check for Orphaned Hosts button clicked");
        orphansStatus.textContent = 'Searching for orphaned hosts...';
        checkOrphansButton.disabled = true;
        orphanedHostsList.innerHTML = ''; // Clear previous list
        relinkFormContainer.style.display = 'none'; // Hide relink form

        try {
            const response = await fetch('/api/orphaned_hosts');
            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.error || `HTTP error! status: ${response.status}`);
            }
            const orphanedHosts = await response.json();
            console.log("Orphaned hosts fetched:", orphanedHosts);

            if (orphanedHosts && orphanedHosts.length > 0) {
                orphansStatus.textContent = `Found ${orphanedHosts.length} orphaned host(s).`;
                orphanedHosts.forEach(host => {
                    const hostDiv = document.createElement('div');
                    hostDiv.classList.add('list-item');
                    let hint = host.source_host_id_hint ? ` (Hint: Source ID ${host.source_host_id_hint})` : '';
                    hostDiv.innerHTML = `
                        <strong>${host.dest_host_name}</strong> (Dest ID: ${host.dest_host_id})${hint}
                        <button class="btn small-btn relink-host-btn"
                                data-dest-host-id="${host.dest_host_id}"
                                data-dest-host-name="${host.dest_host_name}"
                                data-source-host-id-hint="${host.source_host_id_hint || ''}">
                            Re-link
                        </button>
                    `;
                    orphanedHostsList.appendChild(hostDiv);
                });
                // Add event listeners to the new relink buttons
                document.querySelectorAll('.relink-host-btn').forEach(button => {
                    button.addEventListener('click', (event) => {
                        const destId = event.target.dataset.destHostId;
                        const destName = event.target.dataset.destHostName;
                        const sourceHint = event.target.dataset.sourceHostIdHint;

                        relinkDestHostId.value = destId;
                        relinkDestHostName.value = destName;
                        relinkSourceHostId.value = sourceHint; // Pre-fill with hint
                        relinkStatus.textContent = ''; // Clear previous status
                        relinkFormContainer.style.display = 'block'; // Show the form
                    });
                });

            } else {
                orphansStatus.textContent = 'No orphaned hosts found.';
            }
        } catch (error) {
            console.error("Error checking for orphaned hosts:", error);
            orphansStatus.textContent = `Error: ${error.message || 'Failed to check for orphaned hosts.'}`;
        } finally {
            checkOrphansButton.disabled = false;
        }
    });

    relinkButton.addEventListener('click', async () => {
        const destHostId = relinkDestHostId.value;
        const sourceHostId = relinkSourceHostId.value;
        const destHostName = relinkDestHostName.value;

        if (!destHostId || !sourceHostId) {
            relinkStatus.textContent = 'Please provide both Destination Host ID and Source Host ID.';
            return;
        }

        console.log(`Re-link button clicked for Dest ID: ${destHostId}, Source ID: ${sourceHostId}`);
        relinkStatus.textContent = `Attempting to re-link ${destHostName} (Dest ID: ${destHostId}) to Source ID ${sourceHostId}...`;
        relinkButton.disabled = true;

        try {
            const response = await fetch('/api/relink_host', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ dest_host_id: destHostId, source_host_id: sourceHostId }),
            });

            const data = await response.json();

            if (!response.ok) {
                throw new Error(data.error || `HTTP error! status: ${response.status}`);
            }

            relinkStatus.textContent = data.message || 'Re-linking successful!';
            console.log("Re-linking successful:", data);

            // Update the main replication status list
            updateReplicationStatus(sourceHostId, destHostName, data.message || 'Re-linked and scheduled for replay.');

            // Remove the re-linked host from the orphaned list
            const orphanedHostDiv = document.querySelector(`#orphaned-hosts-list div button[data-dest-host-id="${destHostId}"]`).closest('.list-item');
            if (orphanedHostDiv) {
                orphanedHostDiv.remove();
                if (orphanedHostsList.children.length === 0) {
                    orphansStatus.textContent = 'No orphaned hosts found.';
                }
            }
            relinkFormContainer.style.display = 'none'; // Hide the form after success

        } catch (error) {
            console.error("Error during re-linking:", error);
            relinkStatus.textContent = `Error: ${error.message || 'Failed to re-link host.'}`;
        } finally {
            relinkButton.disabled = false;
        }
    });


    // --- Initial Load: Hide orphaned hosts section and disable button ---
    document.getElementById('orphaned-hosts-section').style.display = 'none';
    checkOrphansButton.disabled = true;

    console.log("Fetching initial replication statuses...");
    try {
        const response = await fetch('/api/replay/status');
        if (!response.ok) {
            console.error(`Initial status fetch: HTTP error! status: ${response.status}`);
            // Display an error message to the user?
            replicationStatusList.innerHTML = '<div>Error loading initial statuses.</div>';
        } else {
            const allStatuses = await response.json();
            console.log("Initial statuses fetched:", allStatuses);

            if (Object.keys(allStatuses).length > 0) {
                statusSection.style.display = 'block'; // Show the status section
                for (const hostId in allStatuses) {
                    const taskStatus = allStatuses[hostId];
                    // We don't have the host name readily available here, use hostId for now
                    // The polling will update with the name once fetched if needed, or we can store it in the task object
                    updateReplicationStatus(hostId, `Host ID ${hostId}`, taskStatus.message || taskStatus.status || 'Unknown status', taskStatus);
                    // Add to active polling if not completed or failed
                    if (taskStatus.status !== 'completed' && taskStatus.status !== 'failed') {
                        activePollingHosts.add(hostId);
                    }
                }
                // Start polling if there are any active tasks loaded
                if (activePollingHosts.size > 0) {
                    startStatusPolling();
                }
            } else {
                // No existing tasks
                showEmptyState();
            }
        }
    } catch (error) {
        console.error("Initial status fetch error:", error);
        replicationStatusList.innerHTML = `
            <div class="status-empty">
                <i class="fas fa-exclamation-triangle"></i>
                <p>Error loading initial statuses: ${error.message || 'Unknown error'}</p>
            </div>
        `;
    }

});
