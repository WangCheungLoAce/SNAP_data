from flask import Flask, render_template, request, redirect, url_for, send_file
from werkzeug.utils import secure_filename
import subprocess
import os
import zipfile
import pandas as pd
import snaponestep  # Import the snaponestep module

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['DOWNLOAD_FOLDER'] = 'downloads'

# Passcode to allow execution of snap_code.py
PASSCODE = "8888"

def ensure_directories_exist():
    """Ensure that required directories (uploads and downloads) exist."""
    for folder in ['uploads', 'downloads']:
        os.makedirs(folder, exist_ok=True)

def extract_and_combine(zip_file_path):
    """Extract CSV data from a ZIP file and combine into a single DataFrame."""
    dfs = []
    with zipfile.ZipFile(zip_file_path, 'r') as zf:
        for filename in zf.namelist():
            if filename.lower().endswith('.csv'):
                with zf.open(filename) as f:
                    df = pd.read_csv(f)
                    dfs.append(df)
    raw_df = pd.concat(dfs, ignore_index=True)
    return raw_df

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/rundownload', methods=['POST'])
def run_download():
    try:
        result = subprocess.run(['python', '1_web.py'], capture_output=True, text=True)

        if result.returncode == 0:
            output_message = result.stdout.strip()
        else:
            output_message = f"Error: {result.stderr.strip()}"

    except Exception as e:
        output_message = f"Error: {str(e)}"

    return render_template('index.html', output=output_message)

@app.route('/upload', methods=['POST'])
def upload_file():
    ensure_directories_exist()

    if 'file' not in request.files:
        return redirect(request.url)
    
    file = request.files['file']
    if file.filename == '':
        return redirect(request.url)
    
    if file and file.filename.lower().endswith('.zip'):
        zip_path = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(file.filename))
        file.save(zip_path)

        try:
            output_df = extract_and_combine(zip_path)

            csv_filename = 'output.csv'
            csv_path = os.path.join(app.config['DOWNLOAD_FOLDER'], csv_filename)
            output_df.to_csv(csv_path, index=False)

            os.remove(zip_path)

            return render_template('index.html', output=f"CSV file saved: {csv_filename}")

        except Exception as e:
            output_message = f"Error processing file: {str(e)}"
            return render_template('index.html', output=output_message)

    return render_template('index.html', output="No valid file uploaded.")

@app.route('/download', methods=['GET'])
def download_file():
    csv_filename = 'output.csv'
    csv_path = os.path.join(app.config['DOWNLOAD_FOLDER'], csv_filename)
    return send_file(csv_path, as_attachment=True, download_name=csv_filename)

@app.route('/run_cleanup', methods=['POST'])
def run_cleanup():
    if request.method == 'POST':
        proceed = request.form['confirmation'].lower()

        if proceed == "yes":
            try:
                result = subprocess.run(['python', '3_clean.py'], capture_output=True, text=True)

                if result.returncode == 0:
                    output_message = result.stdout.strip()
                else:
                    output_message = f"Error: {result.stderr.strip()}"

                return render_template('index.html', output=output_message)

            except Exception as e:
                output_message = f"Error: {str(e)}"
                return render_template('index.html', output=output_message)

        else:
            output_message = "No updates performed."
            return render_template('index.html', output=output_message)

@app.route('/azureupload', methods=['POST'])
def azure_upload():
    if request.method == 'POST':
        account_name = request.form['account_name']
        account_key = request.form['account_key']
        container_name = request.form['container_name']
        file_name = request.form['file_name'] 
        # Execute azureupload.py with provided parameters
        try:
            output = subprocess.check_output(['python', 'azureupload.py', 
                                              account_name, account_key, container_name, file_name], 
                                             universal_newlines=True)
            # Filter output to get only the relevant part
            relevant_output = output.split('df.info()')[0]
        except subprocess.CalledProcessError as e:
            relevant_output = f"Error: {e.output}"
        
        return render_template('index.html', output=relevant_output)
    
@app.route('/postgresupload', methods=['POST'])
def postgres_upload():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        host = request.form['host']
        port = request.form['port']
        database_name = request.form['database_name']
        
        try:
            # Construct the command to call postgresupload.py with parameters and files
            command = ['python', 'postgresupload.py', username, password, host, port, database_name]
            
            # Get list of uploaded files with the name 'files[]'
            uploaded_files = request.files.getlist('files[]')
            
            # Append file paths to the command
            for file in uploaded_files:
                if file.filename.endswith('.csv'):
                    file_path = f"uploads/{file.filename}"  # Save uploaded files to 'uploads' directory
                    file.save(file_path)  # Save the file to disk
                    command.append(file_path)  # Append file path to the command
            
            # Execute the command using subprocess
            output = subprocess.check_output(command, universal_newlines=True)
            
            return render_template('index.html', output=output)
        
        except subprocess.CalledProcessError as e:
            return render_template('index.html', output=f"Error: {e.output}")
        
@app.route('/runcode', methods=['POST'])
def run_code():
    passcode = request.form.get('passcode')
    if passcode == '8888':  # Your desired passcode
        snaponestep.main()  # Call the main function from snaponestep.py
        return "Process completed successfully!"
    else:
        return "Unauthorized", 401



if __name__ == '__main__':
    app.run(debug=True)