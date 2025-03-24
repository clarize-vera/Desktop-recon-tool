import os
import sys
import pandas as pd
import pdfplumber
import re
from fuzzywuzzy import process
import tempfile
from datetime import datetime
import tkinter as tk
from tkinter import filedialog, ttk, messagebox
from ttkthemes import ThemedTk
import threading
import queue

class ReconciliationTool:
    def __init__(self, root):
        self.root = root
        self.root.title("Transaction Reconciliation Tool")
        self.root.geometry("800x600")
        self.root.minsize(800, 600)
        
        self.pdf_folder_path = tk.StringVar()
        self.excel_file_path = tk.StringVar()
        self.second_excel_file_path = tk.StringVar()
        self.output_folder_path = tk.StringVar()
        self.status_var = tk.StringVar()
        self.status_var.set("Ready")
        self.progress_var = tk.DoubleVar()
        self.progress_var.set(0)
        
        # Mode selection
        self.reconciliation_mode = tk.StringVar()
        self.reconciliation_mode.set("pdf_excel")  # Default mode
        
        self.queue = queue.Queue()
        
        self.create_widgets()
        self.check_queue()
    
    def create_widgets(self):
        main_frame = ttk.Frame(self.root, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Title
        title_label = ttk.Label(main_frame, text="Transaction Reconciliation Tool", font=("Arial", 16, "bold"))
        title_label.pack(pady=(0, 20))
        
        # Mode Selection Frame
        mode_frame = ttk.LabelFrame(main_frame, text="Reconciliation Mode", padding="10")
        mode_frame.pack(fill=tk.X, pady=10)
        
        ttk.Radiobutton(mode_frame, text="PDF Statements with Excel File", 
                        variable=self.reconciliation_mode, value="pdf_excel",
                        command=self.update_input_fields).pack(anchor=tk.W, pady=5)
        
        ttk.Radiobutton(mode_frame, text="Excel File with Excel File", 
                        variable=self.reconciliation_mode, value="excel_excel",
                        command=self.update_input_fields).pack(anchor=tk.W, pady=5)
        
        # Input Frame
        self.input_frame = ttk.LabelFrame(main_frame, text="Input Files", padding="10")
        self.input_frame.pack(fill=tk.X, pady=10)
        
        # PDF Input Frame (initially visible)
        self.pdf_input_frame = ttk.Frame(self.input_frame)
        self.pdf_input_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(self.pdf_input_frame, text="PDF Statements Folder:").pack(side=tk.LEFT, padx=(0, 10))
        ttk.Entry(self.pdf_input_frame, textvariable=self.pdf_folder_path, width=50).pack(side=tk.LEFT, fill=tk.X, expand=True)
        ttk.Button(self.pdf_input_frame, text="Browse...", command=self.browse_pdf_folder).pack(side=tk.LEFT, padx=(10, 0))
        
        # First Excel File Selection (relabeled based on mode)
        self.excel1_frame = ttk.Frame(self.input_frame)
        self.excel1_frame.pack(fill=tk.X, pady=5)
        
        self.excel1_label = ttk.Label(self.excel1_frame, text="Excel Transactions File:")
        self.excel1_label.pack(side=tk.LEFT, padx=(0, 10))
        ttk.Entry(self.excel1_frame, textvariable=self.excel_file_path, width=50).pack(side=tk.LEFT, fill=tk.X, expand=True)
        ttk.Button(self.excel1_frame, text="Browse...", command=self.browse_excel_file).pack(side=tk.LEFT, padx=(10, 0))
        
        # Second Excel File Selection (initially hidden)
        self.excel2_frame = ttk.Frame(self.input_frame)
        
        ttk.Label(self.excel2_frame, text="Second Excel File:").pack(side=tk.LEFT, padx=(0, 10))
        ttk.Entry(self.excel2_frame, textvariable=self.second_excel_file_path, width=50).pack(side=tk.LEFT, fill=tk.X, expand=True)
        ttk.Button(self.excel2_frame, text="Browse...", command=self.browse_second_excel_file).pack(side=tk.LEFT, padx=(10, 0))
        
        # Output Folder Selection
        output_frame = ttk.Frame(self.input_frame)
        output_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(output_frame, text="Output Folder:").pack(side=tk.LEFT, padx=(0, 10))
        ttk.Entry(output_frame, textvariable=self.output_folder_path, width=50).pack(side=tk.LEFT, fill=tk.X, expand=True)
        ttk.Button(output_frame, text="Browse...", command=self.browse_output_folder).pack(side=tk.LEFT, padx=(10, 0))
        
        # Options Frame
        options_frame = ttk.LabelFrame(main_frame, text="Options", padding="10")
        options_frame.pack(fill=tk.X, pady=10)
        
        # Add options here if needed
        
        # Action Buttons
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X, pady=10)
        
        ttk.Button(button_frame, text="Run Reconciliation", command=self.run_reconciliation, style="Accent.TButton").pack(side=tk.RIGHT)
        ttk.Button(button_frame, text="Exit", command=self.root.destroy).pack(side=tk.RIGHT, padx=10)
        
        # Status Frame
        status_frame = ttk.Frame(main_frame)
        status_frame.pack(fill=tk.X, side=tk.BOTTOM, pady=10)
        
        ttk.Label(status_frame, text="Status:").pack(side=tk.LEFT)
        ttk.Label(status_frame, textvariable=self.status_var).pack(side=tk.LEFT, padx=5)
        
        self.progress_bar = ttk.Progressbar(status_frame, variable=self.progress_var, mode="determinate", length=200)
        self.progress_bar.pack(side=tk.RIGHT)
        
        # Results Frame (initially hidden)
        self.results_frame = ttk.LabelFrame(main_frame, text="Results", padding="10")
        
        self.results_text = tk.Text(self.results_frame, height=10, wrap=tk.WORD)
        results_scrollbar = ttk.Scrollbar(self.results_frame, command=self.results_text.yview)
        self.results_text.configure(yscrollcommand=results_scrollbar.set)
        
        self.results_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        results_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
    
    def update_input_fields(self):
        """Update input fields based on the selected reconciliation mode"""
        mode = self.reconciliation_mode.get()
        
        if mode == "pdf_excel":
            # Show PDF frame, hide second Excel frame
            self.pdf_input_frame.pack(fill=tk.X, pady=5)
            self.excel2_frame.pack_forget()
            self.excel1_label.config(text="Excel Transactions File:")
        else:  # excel_excel mode
            # Hide PDF frame, show second Excel frame
            self.pdf_input_frame.pack_forget()
            self.excel2_frame.pack(fill=tk.X, pady=5)
            self.excel1_label.config(text="First Excel File:")
    
    def browse_pdf_folder(self):
        folder_path = filedialog.askdirectory(title="Select PDF Statements Folder")
        if folder_path:
            self.pdf_folder_path.set(folder_path)
    
    def browse_excel_file(self):
        file_path = filedialog.askopenfilename(
            title="Select Excel File",
            filetypes=[("Excel Files", "*.xlsx *.xls")]
        )
        if file_path:
            self.excel_file_path.set(file_path)
    
    def browse_second_excel_file(self):
        file_path = filedialog.askopenfilename(
            title="Select Second Excel File",
            filetypes=[("Excel Files", "*.xlsx *.xls")]
        )
        if file_path:
            self.second_excel_file_path.set(file_path)
    
    def browse_output_folder(self):
        folder_path = filedialog.askdirectory(title="Select Output Folder")
        if folder_path:
            self.output_folder_path.set(folder_path)
    
    def check_queue(self):
        try:
            message, progress = self.queue.get_nowait()
            self.status_var.set(message)
            if progress is not None:
                self.progress_var.set(progress)
            
            # If we've completed processing
            if progress == 100:
                self.results_frame.pack(fill=tk.BOTH, expand=True, pady=10)
        except queue.Empty:
            pass
        finally:
            self.root.after(100, self.check_queue)
    
    def extract_transactions_from_pdfs(self, folder_path):
        transactions = []
        latest_date = None
        
        # Get list of PDF files
        pdf_files = [f for f in os.listdir(folder_path) if f.lower().endswith('.pdf')]
        total_files = len(pdf_files)
        
        for i, filename in enumerate(pdf_files):
            progress = int((i / total_files) * 50)  # First half of progress bar
            self.queue.put((f"Processing {filename}...", progress))
            
            pdf_path = os.path.join(folder_path, filename)
            try:
                with pdfplumber.open(pdf_path) as pdf:
                    statement_year = None
                    
                    # Try to extract year from filename or first page
                    match = re.search(r'(\d{4})', filename)
                    if match:
                        statement_year = match.group(1)
                    
                    for page_num, page in enumerate(pdf.pages):
                        text = page.extract_text()
                        if text:
                            lines = text.split('\n')
                            
                            # Try to extract statement date from first page
                            if page_num == 0 and not statement_year:
                                date_match = re.search(r'Statement Date\s+(\d{1,2} \w+ (\d{4}))', text)
                                if date_match:
                                    statement_year = date_match.group(2)
                                else:
                                    # Look for any 4-digit year in the text
                                    year_match = re.search(r'(\d{4})', text)
                                    if year_match:
                                        statement_year = year_match.group(1)
                            
                            # If no year found, use current year
                            if not statement_year:
                                statement_year = datetime.now().strftime("%Y")
                            
                            # Try different patterns for transaction extraction
                            for line in lines:
                                # Pattern 1: Date + Description + Amount
                                match1 = re.search(r'(\d{1,2} \w{3})\s+([\w\s\'\-#]+)\s+([0-9,.]+)\s*(Cr)?', line)
                                # Pattern 2: Date + Description + Reference + Amount
                                match2 = re.search(r'(\d{1,2} \w{3})\s+([\w\s\'\-#]+)\s+([A-Za-z0-9@.]+)?\s+([0-9,.]+)\s*(Cr)?', line)
                                
                                match = None
                                amount_index = 0
                                credit_index = 0
                                
                                if match1:
                                    match = match1
                                    amount_index = 3
                                    credit_index = 4
                                elif match2:
                                    match = match2
                                    amount_index = 4
                                    credit_index = 5
                                
                                if match:
                                    date = match.group(1)
                                    description = match.group(2).strip()
                                    amount_str = match.group(amount_index)
                                    credit_indicator = match.group(credit_index) if credit_index < len(match.groups()) + 1 else None
                                    
                                    # Clean amount string and convert to float
                                    amount_str = amount_str.replace(',', '')
                                    try:
                                        amount_value = float(amount_str)
                                        
                                        # Handle credit/debit
                                        if credit_indicator:
                                            amount_value = abs(amount_value)  # Positive for credits
                                        else:
                                            amount_value = -abs(amount_value)  # Negative for debits
                                        
                                        full_date = f"{date} {statement_year}"
                                        
                                        # Parse date
                                        try:
                                            parsed_date = datetime.strptime(full_date, '%d %b %Y')
                                            
                                            # Update latest date
                                            if latest_date is None or parsed_date > latest_date:
                                                latest_date = parsed_date
                                            
                                            transactions.append({
                                                'Transaction Date': full_date,
                                                'Transaction Details': description,
                                                'Amount': amount_value
                                            })
                                        except ValueError:
                                            # Skip if date parsing fails
                                            continue
                                    except ValueError:
                                        # Skip if amount conversion fails
                                        continue
            except Exception as e:
                self.queue.put((f"Error processing {filename}: {str(e)}", None))
        
        # If no transactions were found or date parsing failed
        if not transactions or latest_date is None:
            self.queue.put(("No transactions found or error in parsing dates. Using current date.", None))
            latest_date = datetime.now()
        
        return pd.DataFrame(transactions), latest_date
    
    def reconcile_transactions(self, first_transactions, second_transactions):
        # Ensure Amount columns are float
        first_transactions['Amount'] = first_transactions['Amount'].astype(float)
        second_transactions['Amount'] = second_transactions['Amount'].astype(float)
        
        # Create a copy for matched transactions
        reconciled_df = first_transactions.copy()
        
        # Use fuzzy matching to find similar amounts
        reconciled_df['Match'] = reconciled_df['Amount'].apply(
            lambda amt: process.extractOne(str(amt), second_transactions['Amount'].astype(str), score_cutoff=90))
        
        reconciled_df['Matched Amount'] = reconciled_df['Match'].apply(lambda x: x[0] if x else 'No Match')
        reconciled_df['Match Score'] = reconciled_df['Match'].apply(lambda x: x[1] if x else 0)
        reconciled_df.drop(columns=['Match'], inplace=True)
        
        # Find transactions only in first source
        matched_amounts = reconciled_df[reconciled_df['Matched Amount'] != 'No Match']['Matched Amount'].tolist()
        only_in_first = reconciled_df[reconciled_df['Matched Amount'] == 'No Match']
        
        # Find transactions only in second source
        only_in_second = second_transactions[~second_transactions['Amount'].astype(str).isin(matched_amounts)]
        
        return reconciled_df, only_in_second, only_in_first
    
    def load_excel_to_dataframe(self, file_path, is_second_file=False):
        """Load Excel file and normalize its structure for reconciliation"""
        df = pd.read_excel(file_path)
        
        # Check if the required columns exist, if not try to identify them
        required_columns = ['Transaction Date', 'Transaction Details', 'Amount']
        
        # If the DataFrame doesn't have the required columns, try to map them
        if not all(col in df.columns for col in required_columns):
            # Create a mapping of common column names
            date_columns = ['Date', 'TransactionDate', 'Transaction Date', 'date']
            details_columns = ['Description', 'Details', 'Transaction Details', 'Narration', 'Reference', 'Payee']
            amount_columns = ['Amount', 'Value', 'Debit/Credit', 'Sum', 'Total']
            
            # Find the best match for each required column
            for req_col, possible_cols in [
                ('Transaction Date', date_columns),
                ('Transaction Details', details_columns),
                ('Amount', amount_columns)
            ]:
                if req_col not in df.columns:
                    for col in possible_cols:
                        if col in df.columns:
                            df.rename(columns={col: req_col}, inplace=True)
                            break
        
        # Ensure the dataframe has the required columns
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            # If it's the second file for Excel-Excel reconciliation, be more lenient
            # Create empty columns for missing ones to continue with reconciliation
            if is_second_file:
                for col in missing_columns:
                    df[col] = "Unknown" if col != "Amount" else 0.0
                return df
            else:
                raise ValueError(f"Required columns not found in Excel file: {missing_columns}. "
                                 f"File should have columns for date, description, and amount.")
        
        return df
    
    def run_reconciliation(self):
        mode = self.reconciliation_mode.get()
        
        # Validate inputs based on mode
        if mode == "pdf_excel":
            if not self.pdf_folder_path.get():
                messagebox.showerror("Error", "Please select a PDF statements folder")
                return
            
            if not self.excel_file_path.get():
                messagebox.showerror("Error", "Please select an Excel transactions file")
                return
        else:  # excel_excel mode
            if not self.excel_file_path.get():
                messagebox.showerror("Error", "Please select the first Excel file")
                return
            
            if not self.second_excel_file_path.get():
                messagebox.showerror("Error", "Please select the second Excel file")
                return
        
        if not self.output_folder_path.get():
            messagebox.showerror("Error", "Please select an output folder")
            return
        
        # Clear previous results
        self.results_text.delete(1.0, tk.END)
        self.progress_var.set(0)
        
        # Run reconciliation in a separate thread
        threading.Thread(target=self._process_reconciliation, daemon=True).start()
    
    def _process_reconciliation(self):
        try:
            mode = self.reconciliation_mode.get()
            
            if mode == "pdf_excel":
                # Step 1: Extract transactions from PDFs
                self.queue.put(("Extracting transactions from PDF statements...", 10))
                first_transactions, latest_date = self.extract_transactions_from_pdfs(self.pdf_folder_path.get())
                source1_name = "PDF Statements"
                
                # Step 2: Load Excel transactions
                self.queue.put(("Loading Excel transactions...", 60))
                second_transactions = self.load_excel_to_dataframe(self.excel_file_path.get())
                source2_name = "Excel File"
            else:  # excel_excel mode
                # Step 1: Load first Excel file
                self.queue.put(("Loading first Excel file...", 30))
                first_transactions = self.load_excel_to_dataframe(self.excel_file_path.get())
                source1_name = "First Excel File"
                
                # Get latest date from the first Excel file
                if 'Transaction Date' in first_transactions.columns:
                    try:
                        # Try to parse dates
                        date_col = pd.to_datetime(first_transactions['Transaction Date'])
                        latest_date = date_col.max()
                    except:
                        latest_date = datetime.now()
                else:
                    latest_date = datetime.now()
                
                # Step 2: Load second Excel file
                self.queue.put(("Loading second Excel file...", 60))
                second_transactions = self.load_excel_to_dataframe(self.second_excel_file_path.get(), is_second_file=True)
                source2_name = "Second Excel File"
            
            # Step 3: Reconcile transactions
            self.queue.put(("Reconciling transactions...", 70))
            reconciled_df, only_in_second, only_in_first = self.reconcile_transactions(first_transactions, second_transactions)
            
            # Step 4: Calculate balance
            self.queue.put(("Calculating balance differences...", 80))
            total_first = first_transactions['Amount'].sum()
            total_second = second_transactions['Amount'].sum()
            balance_difference = total_second - total_first
            
            balance_df = pd.DataFrame({
                'Date': [latest_date.strftime('%d %b %Y') if hasattr(latest_date, 'strftime') else str(latest_date)],
                'Description': ['Balance Difference'],
                'Amount': [balance_difference]
            })
            
            # Step 5: Save results
            self.queue.put(("Saving results...", 90))
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            result_path = os.path.join(self.output_folder_path.get(), f"Reconciliation_Result_{timestamp}.xlsx")
            extracted_path = os.path.join(self.output_folder_path.get(), f"Source1_Transactions_{timestamp}.xlsx")
            
            with pd.ExcelWriter(result_path) as writer:
                reconciled_df.to_excel(writer, sheet_name='Matched Transactions', index=False)
                only_in_second.to_excel(writer, sheet_name=f'In {source2_name} Only', index=False)
                only_in_first.to_excel(writer, sheet_name=f'In {source1_name} Only', index=False)
                balance_df.to_excel(writer, sheet_name='Balance Differences', index=False)
            
            first_transactions.to_excel(extracted_path, index=False)
            
            # Step 6: Display summary
            self.queue.put(("Reconciliation completed successfully!", 100))
            
            summary = (
                f"Reconciliation completed successfully!\n\n"
                f"Total transactions from {source1_name}: {len(first_transactions)}\n"
                f"Total transactions from {source2_name}: {len(second_transactions)}\n"
                f"Matched transactions: {len(reconciled_df) - len(only_in_first)}\n"
                f"Only in {source1_name}: {len(only_in_first)}\n"
                f"Only in {source2_name}: {len(only_in_second)}\n\n"
                f"Total amount in {source1_name}: {total_first:.2f}\n"
                f"Total amount in {source2_name}: {total_second:.2f}\n"
                f"Balance difference: {balance_difference:.2f}\n\n"
                f"Results saved to:\n"
                f"- {result_path}\n"
                f"- {extracted_path}"
            )
            
            self.root.after(0, lambda: self.results_text.insert(tk.END, summary))
            
        except Exception as e:
            error_message = f"Error during reconciliation: {str(e)}"
            self.queue.put((error_message, 100))
            self.root.after(0, lambda: self.results_text.insert(tk.END, error_message))

def main():
    root = ThemedTk(theme="arc")  # Use a modern theme
    app = ReconciliationTool(root)
    root.mainloop()

if __name__ == "__main__":
    main()
