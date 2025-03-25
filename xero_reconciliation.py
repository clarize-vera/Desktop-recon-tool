import os
import sys
import traceback
from datetime import datetime
import threading
import queue
import re
import tempfile

import pandas as pd
import pdfplumber
import tkinter as tk
from tkinter import filedialog, ttk, messagebox
from fuzzywuzzy import process

# Set pandas display options
pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)

# Try to import additional packages with fallbacks
try:
    from ttkthemes import ThemedTk
    TTKTHEMES_AVAILABLE = True
except ImportError:
    TTKTHEMES_AVAILABLE = False
    print("ttkthemes package not found. Using standard theme.")
    print("To install ttkthemes, run: pip install ttkthemes")

# Try to import tkcalendar, but provide fallback if not available
try:
    from tkcalendar import DateEntry
    TKCALENDAR_AVAILABLE = True
except ImportError:
    TKCALENDAR_AVAILABLE = False
    print("tkcalendar package not found. Date picker will be replaced with a simple entry widget.")
    print("To install tkcalendar, run: pip install tkcalendar")


class ReconciliationTool:
    def __init__(self, root):
        self.root = root
        self.root.title("Xero Bank Reconciliation Tool")
        self.root.geometry("800x600")
        self.root.minsize(800, 600)
        
        # Initialize variables
        self.pdf_folder_path = tk.StringVar()
        self.excel_file_path = tk.StringVar()
        self.second_excel_file_path = tk.StringVar()
        self.output_folder_path = tk.StringVar()
        self.status_var = tk.StringVar(value="Ready")
        self.progress_var = tk.DoubleVar(value=0)
        
        # Mode selection
        self.reconciliation_mode = tk.StringVar(value="pdf_excel")  # Default mode
        
        # Date range selection
        self.start_date = None
        self.end_date = None
        
        self.queue = queue.Queue()
        
        self.create_widgets()
        self.check_queue()
    
    def create_widgets(self):
        main_frame = ttk.Frame(self.root, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Title
        title_label = ttk.Label(main_frame, text="Xero Bank Reconciliation Tool", font=("Arial", 16, "bold"))
        title_label.pack(pady=(0, 20))
        
        # Mode Selection Frame
        mode_frame = ttk.LabelFrame(main_frame, text="Reconciliation Mode", padding="10")
        mode_frame.pack(fill=tk.X, pady=10)
        
        ttk.Radiobutton(mode_frame, text="PDF Bank Statements with Xero Bank Transactions - Excel", 
                        variable=self.reconciliation_mode, value="pdf_excel",
                        command=self.update_input_fields).pack(anchor=tk.W, pady=5)
        
        ttk.Radiobutton(mode_frame, text="Bank Statements - Excel with Xero Bank Transactions - Excel", 
                        variable=self.reconciliation_mode, value="excel_excel",
                        command=self.update_input_fields).pack(anchor=tk.W, pady=5)
        
        # Date Range Frame
        date_frame = ttk.LabelFrame(main_frame, text="Date Range Selection", padding="10")
        date_frame.pack(fill=tk.X, pady=10)
        
        date_selection_frame = ttk.Frame(date_frame)
        date_selection_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(date_selection_frame, text="Start Date (DD/MM/YYYY):").grid(row=0, column=0, padx=(0, 10), pady=5, sticky=tk.W)
        
        # Create date pickers based on available packages
        if TKCALENDAR_AVAILABLE:
            # Use DateEntry if tkcalendar is available
            self.start_date_picker = DateEntry(date_selection_frame, width=12, background='darkblue',
                                              foreground='white', borderwidth=2, date_pattern='dd/mm/yyyy')
            self.start_date_entry = None
            self.start_date_picker.grid(row=0, column=1, padx=5, pady=5, sticky=tk.W)
        else:
            # Fallback to a standard Entry widget
            self.start_date_var = tk.StringVar(value=datetime.now().strftime('%d/%m/%Y'))
            self.start_date_entry = ttk.Entry(date_selection_frame, width=12, textvariable=self.start_date_var)
            self.start_date_picker = None
            self.start_date_entry.grid(row=0, column=1, padx=5, pady=5, sticky=tk.W)
        
        ttk.Label(date_selection_frame, text="End Date (DD/MM/YYYY):").grid(row=0, column=2, padx=(20, 10), pady=5, sticky=tk.W)
        
        if TKCALENDAR_AVAILABLE:
            # Use DateEntry if tkcalendar is available
            self.end_date_picker = DateEntry(date_selection_frame, width=12, background='darkblue',
                                            foreground='white', borderwidth=2, date_pattern='dd/mm/yyyy')
            self.end_date_entry = None
            self.end_date_picker.grid(row=0, column=3, padx=5, pady=5, sticky=tk.W)
        else:
            # Fallback to a standard Entry widget
            self.end_date_var = tk.StringVar(value=datetime.now().strftime('%d/%m/%Y'))
            self.end_date_entry = ttk.Entry(date_selection_frame, width=12, textvariable=self.end_date_var)
            self.end_date_picker = None
            self.end_date_entry.grid(row=0, column=3, padx=5, pady=5, sticky=tk.W)
        
        # Input Frame
        self.input_frame = ttk.LabelFrame(main_frame, text="Input Files", padding="10")
        self.input_frame.pack(fill=tk.X, pady=10)
        
        # PDF Input Frame (initially visible)
        self.pdf_input_frame = ttk.Frame(self.input_frame)
        self.pdf_input_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(self.pdf_input_frame, text="PDF Bank Statements Folder:").pack(side=tk.LEFT, padx=(0, 10))
        ttk.Entry(self.pdf_input_frame, textvariable=self.pdf_folder_path, width=50).pack(side=tk.LEFT, fill=tk.X, expand=True)
        ttk.Button(self.pdf_input_frame, text="Browse...", command=self.browse_pdf_folder).pack(side=tk.LEFT, padx=(10, 0))
        
        # First Excel File Selection (relabeled based on mode)
        self.excel1_frame = ttk.Frame(self.input_frame)
        self.excel1_frame.pack(fill=tk.X, pady=5)
        
        self.excel1_label = ttk.Label(self.excel1_frame, text="Xero Bank Transactions - Excel:")
        self.excel1_label.pack(side=tk.LEFT, padx=(0, 10))
        ttk.Entry(self.excel1_frame, textvariable=self.excel_file_path, width=50).pack(side=tk.LEFT, fill=tk.X, expand=True)
        ttk.Button(self.excel1_frame, text="Browse...", command=self.browse_excel_file).pack(side=tk.LEFT, padx=(10, 0))
        
        # Second Excel File Selection (initially hidden)
        self.excel2_frame = ttk.Frame(self.input_frame)
        
        ttk.Label(self.excel2_frame, text="Xero Bank Transactions - Excel:").pack(side=tk.LEFT, padx=(0, 10))
        ttk.Entry(self.excel2_frame, textvariable=self.second_excel_file_path, width=50).pack(side=tk.LEFT, fill=tk.X, expand=True)
        ttk.Button(self.excel2_frame, text="Browse...", command=self.browse_second_excel_file).pack(side=tk.LEFT, padx=(10, 0))
        
        # Output Folder Selection
        output_frame = ttk.Frame(self.input_frame)
        output_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(output_frame, text="Output Folder:").pack(side=tk.LEFT, padx=(0, 10))
        ttk.Entry(output_frame, textvariable=self.output_folder_path, width=50).pack(side=tk.LEFT, fill=tk.X, expand=True)
        ttk.Button(output_frame, text="Browse...", command=self.browse_output_folder).pack(side=tk.LEFT, padx=(10, 0))
        
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
            self.excel1_label.config(text="Xero Bank Transactions - Excel:")
        else:  # excel_excel mode
            # Hide PDF frame, show second Excel frame
            self.pdf_input_frame.pack_forget()
            self.excel2_frame.pack(fill=tk.X, pady=5)
            self.excel1_label.config(text="Bank Statements - Excel:")
    
    def browse_pdf_folder(self):
        folder_path = filedialog.askdirectory(title="Select PDF Bank Statements Folder")
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
        """Check queue for status updates from the worker thread"""
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
    
    def run_reconciliation(self):
        """Starts the reconciliation process"""
        # Validate inputs
        if self.reconciliation_mode.get() == "pdf_excel":
            if not self.pdf_folder_path.get() or not self.excel_file_path.get():
                messagebox.showerror("Error", "Please select both a PDF folder and an Excel file.")
                return
        else:  # excel_excel mode
            if not self.excel_file_path.get() or not self.second_excel_file_path.get():
                messagebox.showerror("Error", "Please select both Excel files.")
                return
        
        if not self.output_folder_path.get():
            messagebox.showerror("Error", "Please select an output folder.")
            return
        
        # Get date range
        start_date = None
        end_date = None
        
        try:
            if TKCALENDAR_AVAILABLE:
                start_date = self.start_date_picker.get_date()
                end_date = self.end_date_picker.get_date()
            else:
                start_date = datetime.strptime(self.start_date_var.get(), '%d/%m/%Y')
                end_date = datetime.strptime(self.end_date_var.get(), '%d/%m/%Y')
        except Exception:
            messagebox.showwarning("Warning", "Invalid date format. Using all available dates.")
        
        # Start reconciliation in a separate thread
        threading.Thread(target=self._run_reconciliation_thread, 
                        args=(start_date, end_date), 
                        daemon=True).start()
        
        # Show a message that process has started
        messagebox.showinfo("Info", "Reconciliation process started!")
        self.status_var.set("Reconciliation in progress...")
    
    def _run_reconciliation_thread(self, start_date, end_date):
        """Background thread for reconciliation process"""
        try:
            # Debug information
            print("\n============ RECONCILIATION START ============")
            print(f"Mode: {self.reconciliation_mode.get()}")
            print(f"Start Date: {start_date}")
            print(f"End Date: {end_date}")
            
            mode = self.reconciliation_mode.get()
            output_folder = self.output_folder_path.get()
            
            # Make sure output directory exists
            if not os.path.exists(output_folder):
                os.makedirs(output_folder)
                print(f"Created output directory: {output_folder}")
            
            # Process based on mode
            if mode == "pdf_excel":
                # Process PDFs and reconcile with Excel
                pdf_folder = self.pdf_folder_path.get()
                excel_file = self.excel_file_path.get()
                
                print(f"PDF Folder: {pdf_folder}")
                print(f"Xero Excel File: {excel_file}")
                
                self.queue.put(("Extracting transactions from PDFs...", 10))
                pdf_transactions, latest_date = self.extract_transactions_from_pdfs(pdf_folder, start_date, end_date)
                
                print(f"PDF Transactions extracted: {len(pdf_transactions)}")
                if not pdf_transactions.empty:
                    print(f"PDF columns: {pdf_transactions.columns.tolist()}")
                
                self.queue.put(("Loading Xero transactions from Excel...", 60))
                xero_transactions = self.load_excel_to_dataframe(excel_file, start_date, end_date)
                
                print(f"Xero Transactions loaded: {len(xero_transactions)}")
                if not xero_transactions.empty:
                    print(f"Xero columns: {xero_transactions.columns.tolist()}")
                
                # Check if Amount column exists in both dataframes
                self.validate_dataframes(pdf_transactions, xero_transactions)
                
                self.queue.put(("Reconciling transactions...", 80))
                reconciled, only_in_xero, only_in_pdf = self.reconcile_transactions(pdf_transactions, xero_transactions)
                
                print(f"Reconciliation complete:")
                print(f"- Reconciled: {len(reconciled)}")
                print(f"- Only in Xero: {len(only_in_xero)}")
                print(f"- Only in PDF: {len(only_in_pdf)}")
                
                # Save results
                self.save_results(reconciled, only_in_xero, only_in_pdf, output_folder)
                
                # Display results
                self.root.after(0, lambda: self.display_results(reconciled, only_in_xero, only_in_pdf))
                
            else:  # excel_excel mode
                # Reconcile two Excel files
                first_excel = self.excel_file_path.get()
                second_excel = self.second_excel_file_path.get()
                
                print(f"Bank Excel File: {first_excel}")
                print(f"Xero Excel File: {second_excel}")
                
                self.queue.put(("Loading bank transactions from first Excel...", 20))
                bank_transactions = self.load_excel_to_dataframe(first_excel, start_date, end_date)
                
                print(f"Bank Transactions loaded: {len(bank_transactions)}")
                if not bank_transactions.empty:
                    print(f"Bank columns: {bank_transactions.columns.tolist()}")
                
                self.queue.put(("Loading Xero transactions from second Excel...", 50))
                xero_transactions = self.load_excel_to_dataframe(second_excel, start_date, end_date)
                
                print(f"Xero Transactions loaded: {len(xero_transactions)}")
                if not xero_transactions.empty:
                    print(f"Xero columns: {xero_transactions.columns.tolist()}")
                
                # Check if Amount column exists in both dataframes
                self.validate_dataframes(bank_transactions, xero_transactions)
                
                self.queue.put(("Reconciling transactions...", 80))
                reconciled, only_in_xero, only_in_bank = self.reconcile_transactions(bank_transactions, xero_transactions)
                
                print(f"Reconciliation complete:")
                print(f"- Reconciled: {len(reconciled)}")
                print(f"- Only in Xero: {len(only_in_xero)}")
                print(f"- Only in Bank: {len(only_in_bank)}")
                
                # Save results
                self.save_results(reconciled, only_in_xero, only_in_bank, output_folder)
                
                # Display results
                self.root.after(0, lambda: self.display_results(reconciled, only_in_xero, only_in_bank))
            
            print("============ RECONCILIATION COMPLETE ============\n")
            self.queue.put(("Reconciliation completed successfully!", 100))
            
        except Exception as e:
            error_message = f"Error: {str(e)}"
            print(f"ERROR: {error_message}")
            print(f"Exception type: {type(e).__name__}")
            traceback.print_exc()
            self.queue.put((error_message, 0))
            # Use root.after to show the error message in the UI thread
            self.root.after(0, lambda: messagebox.showerror("Error", f"An error occurred: {str(e)}"))