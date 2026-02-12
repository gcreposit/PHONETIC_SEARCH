# Controllers/PageController.py

from flask import Blueprint, render_template

page_bp = Blueprint(
    "page_bp",
    __name__,
    template_folder="../Templates"  # ensure it points to templates folder
)


@page_bp.route("/tabular-view", methods=["GET"])
def tabluar_view():
    return render_template("tabularView.html")


@page_bp.route("/deduplication-dashboard", methods=["GET"])
def deduplication_dashboard():
    """
    Serve HTML dashboard for voter deduplication management
    """
    html_content = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Voter Deduplication Dashboard</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }

        .container {
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 15px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            overflow: hidden;
        }

        .header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            text-align: center;
        }

        .header h1 {
            font-size: 2.5em;
            margin-bottom: 10px;
        }

        .header p {
            font-size: 1.1em;
            opacity: 0.9;
        }

        .content {
            padding: 30px;
        }

        .section {
            background: #f8f9fa;
            border-radius: 10px;
            padding: 25px;
            margin-bottom: 20px;
            border: 1px solid #e0e0e0;
        }

        .section h2 {
            color: #667eea;
            margin-bottom: 20px;
            font-size: 1.5em;
            border-bottom: 2px solid #667eea;
            padding-bottom: 10px;
        }

        .form-group {
            margin-bottom: 20px;
        }

        .form-group label {
            display: block;
            font-weight: 600;
            margin-bottom: 8px;
            color: #333;
        }

        .form-group select,
        .form-group input[type="number"] {
            width: 100%;
            padding: 12px;
            border: 2px solid #e0e0e0;
            border-radius: 8px;
            font-size: 1em;
            transition: border-color 0.3s;
        }

        .form-group select:focus,
        .form-group input[type="number"]:focus {
            outline: none;
            border-color: #667eea;
        }

        .checkbox-group {
            display: flex;
            align-items: center;
            gap: 10px;
            padding: 15px;
            background: white;
            border-radius: 8px;
            border: 2px solid #e0e0e0;
        }

        .checkbox-group input[type="checkbox"] {
            width: 20px;
            height: 20px;
            cursor: pointer;
        }

        .checkbox-group label {
            margin: 0;
            cursor: pointer;
            font-weight: 500;
        }

        .btn-group {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin-top: 20px;
        }

        .btn {
            padding: 15px 25px;
            border: none;
            border-radius: 8px;
            font-size: 1em;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.3s;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }

        .btn:hover {
            transform: translateY(-2px);
            box-shadow: 0 5px 15px rgba(0,0,0,0.2);
        }

        .btn:active {
            transform: translateY(0);
        }

        .btn-primary {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
        }

        .btn-success {
            background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
            color: white;
        }

        .btn-warning {
            background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
            color: white;
        }

        .btn-danger {
            background: linear-gradient(135deg, #fc4a1a 0%, #f7b733 100%);
            color: white;
        }

        .btn-info {
            background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
            color: white;
        }

        .btn:disabled {
            opacity: 0.5;
            cursor: not-allowed;
        }

        .results {
            margin-top: 30px;
            padding: 20px;
            background: white;
            border-radius: 10px;
            border: 2px solid #e0e0e0;
            display: none;
        }

        .results.show {
            display: block;
        }

        .results h3 {
            color: #667eea;
            margin-bottom: 15px;
        }

        .stats {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 15px;
            margin-bottom: 20px;
        }

        .stat-card {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 10px;
            text-align: center;
        }

        .stat-card .number {
            font-size: 2em;
            font-weight: bold;
            margin-bottom: 5px;
        }

        .stat-card .label {
            font-size: 0.9em;
            opacity: 0.9;
        }

        .duplicate-list {
            max-height: 500px;
            overflow-y: auto;
            margin-top: 20px;
        }

        .duplicate-item {
            background: #f8f9fa;
            padding: 15px;
            margin-bottom: 15px;
            border-radius: 8px;
            border-left: 4px solid #667eea;
        }

        .duplicate-item .primary {
            font-weight: 600;
            color: #11998e;
            margin-bottom: 10px;
        }

        .duplicate-item .duplicates {
            padding-left: 20px;
            color: #f5576c;
        }

        .duplicate-item .dup-record {
            padding: 8px;
            background: white;
            margin: 5px 0;
            border-radius: 5px;
            font-size: 0.9em;
        }

        .loading {
            text-align: center;
            padding: 40px;
            display: none;
        }

        .loading.show {
            display: block;
        }

        .spinner {
            border: 4px solid #f3f3f3;
            border-top: 4px solid #667eea;
            border-radius: 50%;
            width: 50px;
            height: 50px;
            animation: spin 1s linear infinite;
            margin: 0 auto 20px;
        }

        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }

        .alert {
            padding: 15px;
            border-radius: 8px;
            margin-bottom: 20px;
            display: none;
        }

        .alert.show {
            display: block;
        }

        .alert-success {
            background: #d4edda;
            color: #155724;
            border: 1px solid #c3e6cb;
        }

        .alert-error {
            background: #f8d7da;
            color: #721c24;
            border: 1px solid #f5c6cb;
        }

        .alert-warning {
            background: #fff3cd;
            color: #856404;
            border: 1px solid #ffeaa7;
        }

        .info-box {
            background: #e7f3ff;
            border-left: 4px solid #2196F3;
            padding: 15px;
            border-radius: 5px;
            margin-top: 15px;
        }

        .info-box strong {
            color: #2196F3;
        }

        pre {
            background: #2d3748;
            color: #68d391;
            padding: 15px;
            border-radius: 8px;
            overflow-x: auto;
            font-size: 0.9em;
        }

        .json-output {
            max-height: 400px;
            overflow-y: auto;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🔍 Voter Deduplication Dashboard</h1>
            <p>Phonetic Matching System for Hindi/English Names</p>
        </div>

        <div class="content">
            <!-- Alert Messages -->
            <div id="alertBox" class="alert"></div>

            <!-- Configuration Section -->
            <div class="section">
                <h2>⚙️ Configuration</h2>

                <div class="form-group">
                    <label for="tableSelect">Select Table:</label>
                    <select id="tableSelect">
                        <option value="gram_panchayat_voters">Gram Panchayat Voters</option>
                        <option value="nagar_nigam">Nagar Nigam</option>
                        <option value="voters_pdf_extract">Voters PDF Extract</option>
                        <option value="voter_data">Voter Data</option>
                        <option value="testing">Testing Table</option>
                    </select>
                </div>

                <div class="form-group">
                    <label for="threshold">Similarity Threshold (0-100):</label>
                    <input type="number" id="threshold" min="0" max="100" value="85" step="5">
                    <div class="info-box">
                        <strong>Threshold Guide:</strong>
                        <ul style="margin-top: 10px; margin-left: 20px;">
                            <li><strong>90-100:</strong> Very strict (exact phonetic match)</li>
                            <li><strong>80-89:</strong> Strict (recommended for production)</li>
                            <li><strong>70-79:</strong> Moderate (catches more variations)</li>
                            <li><strong>Below 70:</strong> Loose (may have false positives)</li>
                        </ul>
                    </div>
                </div>

                <div class="form-group">
                    <label for="previewLimit">Preview Limit:</label>
                    <input type="number" id="previewLimit" min="10" max="1000" value="50" step="10">
                </div>

                <div class="form-group">
                    <div class="checkbox-group">
                        <input type="checkbox" id="dryRun" checked>
                        <label for="dryRun">🛡️ Dry Run Mode (Preview only - No database changes)</label>
                    </div>
                </div>
            </div>

            <!-- Action Buttons -->
            <div class="section">
                <h2>🎯 Actions</h2>
                <div class="btn-group">
                    <button class="btn btn-info" onclick="previewDuplicates()">
                        👁️ Preview Duplicates
                    </button>
                    <button class="btn btn-primary" onclick="runDeduplication()">
                        🔄 Run Deduplication
                    </button>
                    <button class="btn btn-success" onclick="getStatistics()">
                        📊 View Statistics
                    </button>
                    <button class="btn btn-warning" onclick="resetToActive()">
                        ♻️ Reset to Active
                    </button>
                    <button class="btn btn-danger" onclick="clearResults()">
                        🗑️ Clear Results
                    </button>
                </div>
            </div>

            <!-- Loading Indicator -->
            <div id="loading" class="loading">
                <div class="spinner"></div>
                <p>Processing... Please wait</p>
            </div>

            <!-- Results Section -->
            <div id="results" class="results">
                <h3>📋 Results</h3>
                <div id="statsContainer" class="stats"></div>
                <div id="duplicateList" class="duplicate-list"></div>
                <div id="jsonOutput" class="json-output"></div>
            </div>
        </div>
    </div>

    <script>
        const API_BASE = '/api/pysearch';

        function showAlert(message, type = 'success') {
            const alertBox = document.getElementById('alertBox');
            alertBox.className = `alert alert-${type} show`;
            alertBox.textContent = message;
            setTimeout(() => {
                alertBox.classList.remove('show');
            }, 5000);
        }

        function showLoading() {
            document.getElementById('loading').classList.add('show');
        }

        function hideLoading() {
            document.getElementById('loading').classList.remove('show');
        }

        function showResults() {
            document.getElementById('results').classList.add('show');
        }

        function clearResults() {
            document.getElementById('results').classList.remove('show');
            document.getElementById('statsContainer').innerHTML = '';
            document.getElementById('duplicateList').innerHTML = '';
            document.getElementById('jsonOutput').innerHTML = '';
            showAlert('Results cleared', 'success');
        }

        async function previewDuplicates() {
            const table = document.getElementById('tableSelect').value;
            const threshold = document.getElementById('threshold').value;
            const limit = document.getElementById('previewLimit').value;

            showLoading();
            try {
                const response = await fetch(`${API_BASE}/preview-duplicates?table=${table}&threshold=${threshold}&limit=${limit}`);
                const data = await response.json();

                hideLoading();
                showResults();

                // Display Statistics
                const statsHtml = `
                    <div class="stat-card">
                        <div class="number">${data.total_duplicate_groups}</div>
                        <div class="label">Duplicate Groups</div>
                    </div>
                    <div class="stat-card">
                        <div class="number">${data.preview_count}</div>
                        <div class="label">Previewed</div>
                    </div>
                    <div class="stat-card">
                        <div class="number">${data.threshold_used}%</div>
                        <div class="label">Threshold</div>
                    </div>
                `;
                document.getElementById('statsContainer').innerHTML = statsHtml;

                // Display Duplicate List
                let listHtml = '<h4 style="margin-bottom: 15px;">Duplicate Records Found:</h4>';
                if (data.data && data.data.length > 0) {
                    data.data.forEach((group, index) => {
                        listHtml += `
                            <div class="duplicate-item">
                                <div class="primary">
                                    <strong>Primary Record (ID: ${group.primary_record.id}):</strong><br>
                                    👤 ${group.primary_record.voter_name}<br>
                                    👨‍👩‍👦 ${group.primary_record.father_name}
                                </div>
                                <div class="duplicates">
                                    <strong>Duplicates (${group.duplicates.length}):</strong>
                                    ${group.duplicates.map(dup => `
                                        <div class="dup-record">
                                            <strong>ID: ${dup.id}</strong> | Match: ${dup.match_score}%<br>
                                            👤 ${dup.voter_name}<br>
                                            👨‍👩‍👦 ${dup.father_name}
                                        </div>
                                    `).join('')}
                                </div>
                            </div>
                        `;
                    });
                } else {
                    listHtml += '<p>No duplicates found! 🎉</p>';
                }
                document.getElementById('duplicateList').innerHTML = listHtml;

                // Display JSON
                document.getElementById('jsonOutput').innerHTML = `
                    <h4 style="margin-top: 20px; margin-bottom: 10px;">Raw JSON Response:</h4>
                    <pre>${JSON.stringify(data, null, 2)}</pre>
                `;

                showAlert(`Found ${data.total_duplicate_groups} duplicate groups`, 'success');
            } catch (error) {
                hideLoading();
                showAlert(`Error: ${error.message}`, 'error');
            }
        }

        async function runDeduplication() {
            const table = document.getElementById('tableSelect').value;
            const threshold = document.getElementById('threshold').value;
            const dryRun = document.getElementById('dryRun').checked;

            const confirmMsg = dryRun 
                ? 'Run deduplication in DRY RUN mode (no changes will be made)?' 
                : '⚠️ WARNING: This will PERMANENTLY mark duplicates as INACTIVE. Continue?';

            if (!confirm(confirmMsg)) return;

            showLoading();
            try {
                const response = await fetch(`${API_BASE}/deduplicate-voters`, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({
                        table_name: table,
                        threshold: parseInt(threshold),
                        dry_run: dryRun
                    })
                });

                const data = await response.json();
                hideLoading();
                showResults();

                // Display Statistics
                const statsHtml = `
                    <div class="stat-card">
                        <div class="number">${data.total_records_processed}</div>
                        <div class="label">Total Processed</div>
                    </div>
                    <div class="stat-card">
                        <div class="number">${data.duplicate_groups_found}</div>
                        <div class="label">Duplicate Groups</div>
                    </div>
                    <div class="stat-card">
                        <div class="number">${data.records_to_deactivate}</div>
                        <div class="label">To Deactivate</div>
                    </div>
                    <div class="stat-card">
                        <div class="number">${dryRun ? 'YES' : 'NO'}</div>
                        <div class="label">Dry Run</div>
                    </div>
                `;
                document.getElementById('statsContainer').innerHTML = statsHtml;

                // Display records to deactivate
                let listHtml = '<h4 style="margin-bottom: 15px;">Records to be Deactivated:</h4>';
                if (data.details && data.details.length > 0) {
                    data.details.forEach(rec => {
                        listHtml += `
                            <div class="duplicate-item">
                                <strong>ID: ${rec.id}</strong> (Duplicate of ID: ${rec.duplicate_of})<br>
                                Match Score: ${rec.match_score}%<br>
                                👤 ${rec.voter_name}<br>
                                👨‍👩‍👦 ${rec.father_name}
                            </div>
                        `;
                    });
                } else {
                    listHtml += '<p>No records to deactivate! 🎉</p>';
                }
                document.getElementById('duplicateList').innerHTML = listHtml;

                // Display JSON
                document.getElementById('jsonOutput').innerHTML = `
                    <h4 style="margin-top: 20px; margin-bottom: 10px;">Full Response:</h4>
                    <pre>${JSON.stringify(data, null, 2)}</pre>
                `;

                const msg = dryRun 
                    ? `Dry run completed! Found ${data.records_to_deactivate} duplicates` 
                    : `Successfully marked ${data.records_to_deactivate} records as INACTIVE`;
                showAlert(msg, dryRun ? 'warning' : 'success');

            } catch (error) {
                hideLoading();
                showAlert(`Error: ${error.message}`, 'error');
            }
        }

        async function getStatistics() {
            const table = document.getElementById('tableSelect').value;

            showLoading();
            try {
                const response = await fetch(`${API_BASE}/statistics?table=${table}`);
                const data = await response.json();

                hideLoading();
                showResults();

                const statsHtml = `
                    <div class="stat-card">
                        <div class="number">${data.total_records}</div>
                        <div class="label">Total Records</div>
                    </div>
                    <div class="stat-card">
                        <div class="number">${data.active_records}</div>
                        <div class="label">Active</div>
                    </div>
                    <div class="stat-card">
                        <div class="number">${data.inactive_records}</div>
                        <div class="label">Inactive</div>
                    </div>
                    <div class="stat-card">
                        <div class="number">${data.duplicate_percentage}%</div>
                        <div class="label">Duplicates</div>
                    </div>
                `;
                document.getElementById('statsContainer').innerHTML = statsHtml;

                document.getElementById('jsonOutput').innerHTML = `
                    <h4 style="margin-top: 20px; margin-bottom: 10px;">Statistics:</h4>
                    <pre>${JSON.stringify(data, null, 2)}</pre>
                `;

                showAlert('Statistics loaded successfully', 'success');
            } catch (error) {
                hideLoading();
                showAlert(`Error: ${error.message}`, 'error');
            }
        }

        async function resetToActive() {
            const table = document.getElementById('tableSelect').value;

            if (!confirm('⚠️ WARNING: This will reset ALL records to ACTIVE status. Are you sure?')) {
                return;
            }

            showLoading();
            try {
                const response = await fetch(`${API_BASE}/reset-to-active`, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({
                        table_name: table
                    })
                });

                const data = await response.json();
                hideLoading();
                showResults();

                const statsHtml = `
                    <div class="stat-card">
                        <div class="number">${data.records_reset}</div>
                        <div class="label">Records Reset</div>
                    </div>
                `;
                document.getElementById('statsContainer').innerHTML = statsHtml;

                document.getElementById('jsonOutput').innerHTML = `
                    <h4 style="margin-top: 20px; margin-bottom: 10px;">Reset Result:</h4>
                    <pre>${JSON.stringify(data, null, 2)}</pre>
                `;

                showAlert(`Successfully reset ${data.records_reset} records to ACTIVE`, 'success');
            } catch (error) {
                hideLoading();
                showAlert(`Error: ${error.message}`, 'error');
            }
        }

        // Auto-load statistics on page load
        window.onload = function() {
            console.log('Dashboard loaded successfully!');
        };
    </script>
</body>
</html>
    """
    return html_content

