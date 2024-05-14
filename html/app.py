from flask import Flask, render_template, request, redirect, url_for
import subprocess
import pandas as pd
from tqdm import tqdm

app = Flask(__name__)
# Passcode to allow execution of snap_code.py
PASSCODE = "8888"

# Dictionary to store different types of output
outputs = {
    'web_scraping': None,
}

@app.route('/')
def index():
    return render_template('index.html', outputs=outputs)

@app.route('/webscraping', methods=['POST'])
def run_webscraping():
    try:
        # Run the web_scraping.py script using subprocess
        result = subprocess.run(['python', 'web_scraping.py'], capture_output=True, text=True)

        if result.returncode == 0:
            outputs['web_scraping'] = result.stdout.strip()
        else:
            outputs['web_scraping'] = f"Error: {result.stderr.strip()}"

    except Exception as e:
        outputs['web_scraping'] = f"Error: {str(e)}"

    return render_template('index.html', outputs=outputs)


if __name__ == '__main__':
    app.run(debug=True)