from flask import Flask, render_template, request, redirect, url_for, send_file
import os
import datetime
import gzip
import shutil
import zipfile

app = Flask(__name__)

UPLOAD_FOLDER = "uploads"
EXTRACT_FOLDER = "extracted"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(EXTRACT_FOLDER, exist_ok=True)

TABLE_NAME = "stock_data"

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/upload", methods=["POST"])
def upload_file():
    if "file" not in request.files:
        return "No file uploaded!"

    file = request.files["file"]
    if file.filename == "":
        return "No selected file!"

    file_path = os.path.join(UPLOAD_FOLDER, file.filename)
    file.save(file_path)

    # Get user-selected date in DD-MM-YYYY format
    selected_date = request.form.get("date", datetime.datetime.today().strftime("%d-%m-%Y"))

    # Extract .lis file
    lis_file_path = extract_lis(file_path)

    if not lis_file_path:
        return "Failed to extract .lis file from the compressed archive."

    # Convert LIS to SQL
    sql_file_name = convert_lis_to_sql(lis_file_path, selected_date)

    return redirect(url_for("download_file", filename=sql_file_name))

def extract_lis(file_path):
    """ Detects and extracts LIS file from .Z or .ZIP archive """
    try:
        if zipfile.is_zipfile(file_path):  # If it's a ZIP file
            with zipfile.ZipFile(file_path, "r") as zip_ref:
                for file in zip_ref.namelist():
                    if file.endswith(".lis"):
                        extracted_path = os.path.join(EXTRACT_FOLDER, file)
                        zip_ref.extract(file, EXTRACT_FOLDER)
                        return extracted_path
        else:  # If it's a true .Z file
            lis_filename = os.path.basename(file_path).replace(".Z", ".lis")
            lis_file_path = os.path.join(EXTRACT_FOLDER, lis_filename)

            with gzip.open(file_path, "rb") as f_in:
                with open(lis_file_path, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)

            return lis_file_path if os.path.exists(lis_file_path) else None
    except Exception as e:
        print(f"Error Extracting .lis: {str(e)}")
        return None

def convert_lis_to_sql(file_path, selected_date):
    sql_file_name = f"{selected_date}.sql"

    TABLE_SCHEMA = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        date VARCHAR(10),  
        symbol TEXT,
        open_price FLOAT,
        high FLOAT,
        low FLOAT,
        close FLOAT,
        volume INT,
        ldcp FLOAT,
        UNIQUE(date, symbol)
    );
    """

    sql_statements = [TABLE_SCHEMA]

    with open(file_path, "r") as file:
        for line in file:
            values = line.strip().split("|")
            if len(values) >= 10:
                _, symbol, _, _, open_price, high, low, close, volume, ldcp = values[:10]

                open_price = float(open_price) if open_price else 0.0
                high = float(high) if high else 0.0
                low = float(low) if low else 0.0
                close = float(close) if close else 0.0
                volume = int(volume) if volume else 0
                ldcp = float(ldcp) if ldcp else 0.0

                sql_statements.append(
                    f"""
                    INSERT INTO {TABLE_NAME} (date, symbol, open_price, high, low, close, volume, ldcp)
                    VALUES ('{selected_date}', '{symbol}', {open_price}, {high}, {low}, {close}, {volume}, {ldcp})
                    ON DUPLICATE KEY UPDATE 
                        open_price = VALUES(open_price),
                        high = VALUES(high),
                        low = VALUES(low),
                        close = VALUES(close),
                        volume = VALUES(volume),
                        ldcp = VALUES(ldcp);
                    """
                )

    sql_file_path = os.path.join(UPLOAD_FOLDER, sql_file_name)
    with open(sql_file_path, "w") as sql_file:
        sql_file.write("\n".join(sql_statements))

    return sql_file_name

@app.route("/download/<filename>")
def download_file(filename):
    file_path = os.path.join(UPLOAD_FOLDER, filename)
    return send_file(file_path, as_attachment=True)

if __name__ == "__main__":
    app.run(debug=True)
