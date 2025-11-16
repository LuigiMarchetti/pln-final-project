"""
GUI Application for the Financial News Scraper and Analyzer
"""
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import logging
import threading
from typing import Optional

# Import the new controller that wraps the application logic
from src.financial_analysis.controllers.app_controller import AppController


class LogHandler(logging.Handler):
    """Custom logging handler to redirect logs to a Tkinter Text widget"""

    def __init__(self, text_widget):
        logging.Handler.__init__(self)
        self.text_widget = text_widget

    def emit(self, record):
        msg = self.format(record)

        def append_msg():
            self.text_widget.configure(state='normal')
            self.text_widget.insert(tk.END, msg + '\n')
            self.text_widget.configure(state='disabled')
            self.text_widget.see(tk.END)

        # Ensure thread-safe update to the GUI
        self.text_widget.after(0, append_msg)


class Application(tk.Tk):
    """Main GUI Application Window"""

    def __init__(self):
        super().__init__()
        self.title("Financial News Analyzer")
        #self.geometry("800x700")
        self.attributes('-fullscreen', True)

        self.controller: Optional[AppController] = None
        self.worker_thread: Optional[threading.Thread] = None

        # --- Main Layout ---
        main_frame = ttk.Frame(self, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # --- 1. Input Frame ---
        input_frame = ttk.Labelframe(main_frame, text="Controls", padding="10")
        input_frame.pack(fill=tk.X, expand=False)

        # Ticker
        ttk.Label(input_frame, text="Ticker Symbol:").grid(row=0, column=0, padx=5, pady=5, sticky="w")
        self.ticker_var = tk.StringVar(value="PETR4")
        self.ticker_entry = ttk.Entry(input_frame, textvariable=self.ticker_var, width=15)
        self.ticker_entry.grid(row=0, column=1, padx=5, pady=5, sticky="w")

        # Months
        ttk.Label(input_frame, text="Months Ago:").grid(row=0, column=2, padx=5, pady=5, sticky="w")
        self.months_var = tk.StringVar(value="3")
        self.months_entry = ttk.Entry(input_frame, textvariable=self.months_var, width=5)
        self.months_entry.grid(row=0, column=3, padx=5, pady=5, sticky="w")

        # Language selection
        ttk.Label(input_frame, text="Language:").grid(row=0, column=4, padx=5, pady=5, sticky="w")
        self.language_var = tk.StringVar()
        self.language_combo = ttk.Combobox(
            input_frame,
            textvariable=self.language_var,
            values=["English", "Portuguese"],
            width=12,
            state="readonly"
        )
        self.language_combo.set("English")  # Default value
        self.language_combo.grid(row=0, column=5, padx=5, pady=5, sticky="w")

        # Start Button
        self.start_button = ttk.Button(input_frame, text="Start Analysis", command=self.start_work)
        self.start_button.grid(row=0, column=6, padx=10, pady=5, sticky="e")

        input_frame.grid_columnconfigure(6, weight=1)  # Push button to the right

        # --- 2. Progress Frame ---
        progress_frame = ttk.Labelframe(main_frame, text="Progress", padding="10")
        progress_frame.pack(fill=tk.X, expand=False, pady=5)

        self.progress_bar = ttk.Progressbar(progress_frame, orient="horizontal", mode="determinate")
        self.progress_bar.pack(fill=tk.X, expand=True)

        # --- 3. Results Frame ---
        results_frame = ttk.Labelframe(main_frame, text="Analysis Results", padding="10")
        results_frame.pack(fill=tk.BOTH, expand=True, pady=5)

        self.results_text = scrolledtext.ScrolledText(results_frame, wrap=tk.WORD, height=10, state='disabled')
        self.results_text.pack(fill=tk.BOTH, expand=True)

        # --- 4. Log Frame ---
        log_frame = ttk.Labelframe(main_frame, text="Logs", padding="10")
        log_frame.pack(fill=tk.BOTH, expand=True, pady=5)

        self.log_text = scrolledtext.ScrolledText(log_frame, wrap=tk.WORD, height=10, state='disabled')
        self.log_text.pack(fill=tk.BOTH, expand=True)

        # Configure logging
        self.setup_logging()

    def setup_logging(self):
        """Configure the logging module to output to the GUI"""
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)

        # Remove default handlers
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)

        # Create our GUI handler
        gui_handler = LogHandler(self.log_text)
        gui_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(gui_handler)

        # Also log to console (for debugging)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(console_handler)

    def update_progress(self, value: int):
        """Thread-safe method to update the progress bar"""
        self.progress_bar['value'] = value

    def display_results(self, results: str):
        """Thread-safe method to display final analysis results"""
        self.results_text.configure(state='normal')
        self.results_text.delete(1.0, tk.END)
        self.results_text.insert(tk.END, results)
        self.results_text.configure(state='disabled')

    def start_work(self):
        """Validate inputs and start the worker thread"""

        # --- Input Validation ---
        ticker = self.ticker_var.get().strip().upper()
        if not ticker:
            messagebox.showerror("Error", "Ticker symbol cannot be empty.")
            return

        language = self.language_var.get()
        if not language:
            messagebox.showerror("Error", "Please select an output language.")
            return

        try:
            months = int(self.months_var.get())
            if months <= 0:
                raise ValueError()
        except ValueError:
            messagebox.showerror("Error", "Months Ago must be a positive integer.")
            return

        # --- Check if already running ---
        if self.worker_thread and self.worker_thread.is_alive():
            messagebox.showwarning("Busy", "An analysis is already in progress. Please wait.")
            return

        # --- Disable button and clear old results ---
        self.start_button.configure(state='disabled', text='Running...')
        self.progress_bar['value'] = 0
        self.display_results("")  # Clear previous results
        self.log_text.configure(state='normal')
        self.log_text.delete(1.0, tk.END)
        self.log_text.configure(state='disabled')

        # --- Create controller and start thread ---
        self.controller = AppController(
            ticker=ticker,
            months_ago=months,
            progress_callback=self.update_progress,
            results_callback=self.display_results,
            language=language
        )

        self.worker_thread = threading.Thread(
            target=self.run_controller,
            daemon=True
        )
        self.worker_thread.start()

    def run_controller(self):
        """The target function for the worker thread"""
        try:
            if self.controller:
                self.controller.run()
            logging.info("Analysis task finished.")
        except Exception as e:
            logging.error(f"An unexpected error occurred in the worker thread: {e}", exc_info=True)
            messagebox.showerror("Thread Error", f"A critical error occurred: {e}")
        finally:
            # Re-enable the button once finished
            self.start_button.configure(state='normal', text='Start Analysis')


if __name__ == "__main__":
    app = Application()
    app.mainloop()
