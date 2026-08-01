# -*- coding: utf-8 -*-
"""
Basilisk Database Manager - Graphical User Interface
=====================================================
A PyQt6-based GUI for managing SQLite time series databases.

Features:
- Database creation and connection
- CSV/Excel data upload with preview
- Automatic type inference with manual override support
- Data viewing and export (CSV/Excel)
- Visual query builder with filters, sorting, and limits
- DuckDB analytics integration placeholder
- Encryption and compression options

Usage:
    python Basilisk_GUI.py
"""

import sys
import os
import logging
import pandas as pd
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
                             QPushButton, QLabel, QFileDialog, QTableWidget, QTableWidgetItem, 
                             QTabWidget, QMessageBox, QLineEdit, QComboBox, QGroupBox, 
                             QFormLayout, QSpinBox, QDialog, QDialogButtonBox, QHeaderView,
                             QCheckBox, QSplitter, QTextEdit, QStatusBar, QMenuBar, QMenu, 
                             QProgressBar, QListWidget, QListWidgetItem)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QSettings, QByteArray
from PyQt6.QtGui import QFont, QActionGroup, QIcon, QColor, QPalette, QAction

# Import the core database manager
try:
    from . import SQLiteTimeSeriesManager, DuckDBSyncManager
except ImportError:
    # Fallback mock for standalone testing if module isn't found
    class SQLiteTimeSeriesManager:
        def __init__(self, *args, **kwargs): 
            self.variables = {}
            self.column_types = {}
        def upload_data(self, *args, **kwargs): pass
        def get_all_data(self, *args, **kwargs): return pd.DataFrame()
        def query_data(self, *args, **kwargs): return pd.DataFrame()
        def close(self): pass
    
    class DuckDBSyncManager:
        def __init__(self, *args, **kwargs): pass
        def sync_from_sqlite(self, *args, **kwargs): pass


class WorkerThread(QThread):
    """Background thread for long-running operations to keep UI responsive."""
    finished = pyqtSignal(object)
    error = pyqtSignal(str)
    progress = pyqtSignal(int, str)
    
    def __init__(self, task_func, *args, **kwargs):
        super().__init__()
        self.task_func = task_func
        self.args = args
        self.kwargs = kwargs
    
    def run(self):
        try:
            result = self.task_func(*self.args, **self.kwargs)
            self.finished.emit(result)
        except Exception as e:
            self.error.emit(str(e))


class DatabaseManagerGUI(QMainWindow):
    """Main application window for the Basilisk Database Manager."""
    
    def __init__(self):
        super().__init__()
        self.manager = None
        self.duckdb_manager = None
        self.current_file = None
        self.recent_files = []
        self.load_settings()
        self.init_ui()
        self.init_menu()
        self.statusBar().showMessage("Ready. Create or Open a database to begin.")

    def load_settings(self):
        """Load application settings from previous sessions."""
        self.settings = QSettings("Basilisk", "DatabaseManager")
        self.recent_files = self.settings.value("recent_files", [])
        self.geometry = self.settings.value("geometry", QByteArray())
        self.window_state = self.settings.value("window_state", QByteArray())

    def save_settings(self):
        """Save application settings for next session."""
        self.settings.setValue("recent_files", self.recent_files[-5:])  # Keep last 5
        self.settings.setValue("geometry", self.saveGeometry())
        self.settings.setValue("window_state", self.saveState())

    def init_ui(self):
        """Initialize the main user interface."""
        self.setWindowTitle("🦎 Basilisk Database Manager")
        self.setGeometry(100, 100, 1400, 900)
        
        # Apply modern styling
        self.apply_modern_style()
        
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        main_layout.setSpacing(10)
        main_layout.setContentsMargins(15, 15, 15, 15)
        
        # Tabs
        self.tabs = QTabWidget()
        self.tabs.setDocumentMode(True)
        main_layout.addWidget(self.tabs)
        
        # Tab 1: Setup & Upload
        self.setup_tab = QWidget()
        self.tabs.addTab(self.setup_tab, "📥 Data Upload")
        self.init_setup_tab()
        
        # Tab 2: View Data
        self.view_tab = QWidget()
        self.tabs.addTab(self.view_tab, "👁️ View Data")
        self.init_view_tab()
        
        # Tab 3: Query Builder
        self.query_tab = QWidget()
        self.tabs.addTab(self.query_tab, "🔍 Query Builder")
        self.init_query_tab()
        
        # Tab 4: Analytics (DuckDB)
        self.analytics_tab = QWidget()
        self.tabs.addTab(self.analytics_tab, "📊 Analytics")
        self.init_analytics_tab()
        
        # Status bar with progress
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.progress_bar.setMaximumWidth(200)
        self.statusBar().addPermanentWidget(self.progress_bar)

    def apply_modern_style(self):
        """Apply a modern, professional stylesheet to the application."""
        style_sheet = """
        QMainWindow {
            background-color: #f5f5f5;
        }
        
        QTabWidget::pane {
            border: 1px solid #d0d0d0;
            border-radius: 5px;
            background-color: white;
        }
        
        QTabBar::tab {
            background-color: #e0e0e0;
            padding: 8px 16px;
            margin-right: 2px;
            border-top-left-radius: 5px;
            border-top-right-radius: 5px;
        }
        
        QTabBar::tab:selected {
            background-color: white;
            border-bottom: 2px solid #2196F3;
        }
        
        QTabBar::tab:hover:!selected {
            background-color: #f0f0f0;
        }
        
        QGroupBox {
            font-weight: bold;
            border: 1px solid #d0d0d0;
            border-radius: 5px;
            margin-top: 10px;
            padding-top: 10px;
            background-color: white;
        }
        
        QGroupBox::title {
            subcontrol-origin: margin;
            left: 10px;
            padding: 0 5px;
            color: #2196F3;
        }
        
        QPushButton {
            background-color: #e0e0e0;
            border: none;
            padding: 8px 16px;
            border-radius: 4px;
            font-weight: bold;
        }
        
        QPushButton:hover {
            background-color: #d0d0d0;
        }
        
        QPushButton:pressed {
            background-color: #c0c0c0;
        }
        
        QPushButton[default="true"] {
            background-color: #2196F3;
            color: white;
        }
        
        QPushButton[default="true"]:hover {
            background-color: #1976D2;
        }
        
        QLineEdit, QTextEdit, QComboBox, QSpinBox {
            border: 1px solid #d0d0d0;
            border-radius: 4px;
            padding: 6px;
            background-color: white;
        }
        
        QLineEdit:focus, QTextEdit:focus, QComboBox:focus, QSpinBox:focus {
            border: 2px solid #2196F3;
        }
        
        QTableWidget {
            border: 1px solid #d0d0d0;
            border-radius: 4px;
            background-color: white;
            gridline-color: #e0e0e0;
        }
        
        QTableWidget::item {
            padding: 4px;
        }
        
        QTableWidget::item:selected {
            background-color: #2196F3;
            color: white;
        }
        
        QHeaderView::section {
            background-color: #f5f5f5;
            padding: 8px;
            border: none;
            border-bottom: 2px solid #d0d0d0;
            font-weight: bold;
        }
        
        QStatusBar {
            background-color: #f5f5f5;
            border-top: 1px solid #d0d0d0;
        }
        
        QProgressBar {
            border: 1px solid #d0d0d0;
            border-radius: 4px;
            text-align: center;
        }
        
        QProgressBar::chunk {
            background-color: #2196F3;
        }
        
        QLabel {
            color: #333333;
        }
        """
        self.setStyleSheet(style_sheet)

    def init_setup_tab(self):
        """Initialize the Setup & Upload tab."""
        layout = QVBoxLayout(self.setup_tab)
        layout.setSpacing(15)
        
        # Connection Group
        conn_group = QGroupBox("1. Database Connection")
        conn_layout = QFormLayout()
        conn_layout.setSpacing(10)
        
        self.db_path_input = QLineEdit()
        self.db_path_input.setPlaceholderText("Select a .db file or type a path...")
        btn_browse = QPushButton("📁 Browse...")
        btn_browse.clicked.connect(self.browse_db)
        
        conn_row = QHBoxLayout()
        conn_row.addWidget(self.db_path_input)
        conn_row.addWidget(btn_browse)
        conn_layout.addRow("Database Path:", conn_row)
        
        self.encrypt_check = QCheckBox("Enable Encryption (SQLCipher/AES)")
        self.encrypt_check.setToolTip("Encrypt sensitive data at rest")
        self.compress_check = QCheckBox("Enable Compression (VACUUM)")
        self.compress_check.setToolTip("Compress database after initialization")
        
        conn_layout.addRow(self.encrypt_check)
        conn_layout.addRow(self.compress_check)
        
        btn_connect = QPushButton("🔗 Connect / Create Database")
        btn_connect.clicked.connect(self.connect_db)
        btn_connect.setDefault(True)
        conn_layout.addRow(btn_connect)
        
        conn_group.setLayout(conn_layout)
        layout.addWidget(conn_group)
        
        # Upload Group
        upload_group = QGroupBox("2. Upload Data")
        upload_layout = QFormLayout()
        upload_layout.setSpacing(10)
        
        self.file_path_input = QLineEdit()
        self.file_path_input.setPlaceholderText("Select CSV or Excel file...")
        btn_file = QPushButton("📄 Browse File...")
        btn_file.clicked.connect(self.browse_file)
        
        file_row = QHBoxLayout()
        file_row.addWidget(self.file_path_input)
        file_row.addWidget(btn_file)
        upload_layout.addRow("Data File:", file_row)
        
        self.sheet_combo = QComboBox()
        self.sheet_combo.setEnabled(False)
        self.sheet_combo.addItem("No file loaded")
        upload_layout.addRow("Excel Sheet:", self.sheet_combo)
        
        # Preview section with better layout
        preview_label = QLabel("Preview (First 5 rows):")
        preview_label.setFont(QFont("Arial", 10, QFont.Weight.Bold))
        upload_layout.addRow(preview_label)
        
        self.preview_table = QTableWidget()
        self.preview_table.setMaximumHeight(200)
        self.preview_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        upload_layout.addRow(self.preview_table)
        
        # Type Override Section
        override_group = QGroupBox("Optional: Type Overrides")
        override_layout = QVBoxLayout()
        override_label = QLabel("💡 Leave empty for automatic inference. Format: column_name:TYPE (e.g., price:REAL)")
        override_label.setStyleSheet("color: #666; font-size: 11px; font-style: italic;")
        override_label.setWordWrap(True)
        self.type_override_input = QTextEdit()
        self.type_override_input.setMaximumHeight(80)
        self.type_override_input.setPlaceholderText("Examples:\nvolume:INTEGER\ndescription:TEXT\nprice:REAL\nis_active:INTEGER")
        override_layout.addWidget(override_label)
        override_layout.addWidget(self.type_override_input)
        override_group.setLayout(override_layout)
        upload_layout.addRow(override_group)
        
        btn_upload = QPushButton("🚀 Upload Data")
        btn_upload.clicked.connect(self.upload_data)
        btn_upload.setStyleSheet("""
            QPushButton {
                background-color: #4CAF50; 
                color: white; 
                font-weight: bold; 
                padding: 10px 20px;
                font-size: 12px;
            }
            QPushButton:hover {
                background-color: #45a049;
            }
        """)
        upload_layout.addRow(btn_upload)
        
        upload_group.setLayout(upload_layout)
        layout.addWidget(upload_group)
        
        # Add stretch to push everything up
        layout.addStretch()

    def init_view_tab(self):
        """Initialize the View Data tab."""
        layout = QVBoxLayout(self.view_tab)
        layout.setSpacing(10)
        
        # Toolbar
        toolbar = QHBoxLayout()
        toolbar.setSpacing(10)
        
        btn_refresh = QPushButton("🔄 Refresh")
        btn_refresh.clicked.connect(self.load_data_to_view)
        toolbar.addWidget(btn_refresh)
        
        btn_export_csv = QPushButton("💾 Export CSV")
        btn_export_csv.clicked.connect(lambda: self.export_data("csv"))
        toolbar.addWidget(btn_export_csv)
        
        btn_export_excel = QPushButton("📊 Export Excel")
        btn_export_excel.clicked.connect(lambda: self.export_data("excel"))
        toolbar.addWidget(btn_export_excel)
        
        # Search box
        toolbar.addWidget(QLabel("🔍 Search:"))
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Filter visible data...")
        self.search_input.textChanged.connect(self.filter_table_data)
        self.search_input.setMaximumWidth(200)
        toolbar.addWidget(self.search_input)
        
        toolbar.addStretch()
        layout.addLayout(toolbar)
        
        # Main data table
        self.main_table = QTableWidget()
        self.main_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.main_table.setAlternatingRowColors(True)
        layout.addWidget(self.main_table)
        
        # Row count label
        self.row_count_label = QLabel("Rows: 0")
        self.row_count_label.setStyleSheet("font-weight: bold; color: #666;")
        layout.addWidget(self.row_count_label)

    def init_query_tab(self):
        """Initialize the Query Builder tab."""
        layout = QVBoxLayout(self.query_tab)
        layout.setSpacing(15)
        
        # Filter Builder
        filter_group = QGroupBox("Build Filter Conditions")
        filter_layout = QFormLayout()
        filter_layout.setSpacing(10)
        
        filter_instructions = QLabel("Add conditions to filter your data. Multiple conditions are combined with AND.")
        filter_instructions.setStyleSheet("color: #666; font-style: italic;")
        filter_instructions.setWordWrap(True)
        filter_layout.addRow(filter_instructions)
        
        row = QHBoxLayout()
        row.setSpacing(10)
        
        self.filter_column = QComboBox()
        self.filter_column.setMinimumWidth(150)
        self.filter_op = QComboBox()
        self.filter_op.addItems(["=", "!=", ">", "<", ">=", "<=", "LIKE", "NOT LIKE"])
        self.filter_op.setMinimumWidth(80)
        self.filter_value = QLineEdit()
        self.filter_value.setPlaceholderText("Value...")
        self.filter_value.setMinimumWidth(150)
        
        row.addWidget(QLabel("Column:"))
        row.addWidget(self.filter_column)
        row.addWidget(QLabel("Operator:"))
        row.addWidget(self.filter_op)
        row.addWidget(QLabel("Value:"))
        row.addWidget(self.filter_value)
        
        filter_layout.addRow(row)
        
        btn_row = QHBoxLayout()
        btn_add_filter = QPushButton("➕ Add Condition")
        btn_add_filter.clicked.connect(self.add_filter_condition)
        btn_add_filter.setMaximumWidth(150)
        btn_row.addWidget(btn_add_filter)
        
        btn_clear_filters = QPushButton("🗑️ Clear All")
        btn_clear_filters.clicked.connect(self.clear_filters)
        btn_clear_filters.setMaximumWidth(150)
        btn_row.addWidget(btn_clear_filters)
        
        btn_row.addStretch()
        filter_layout.addRow(btn_row)
        
        self.filters_list = QTableWidget()
        self.filters_list.setColumnCount(4)
        self.filters_list.setHorizontalHeaderLabels(["Column", "Operator", "Value", "Action"])
        self.filters_list.setMaximumHeight(180)
        self.filters_list.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.filters_list.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        filter_layout.addRow(self.filters_list)
        
        filter_group.setLayout(filter_layout)
        layout.addWidget(filter_group)
        
        # Sort & Limit
        sort_group = QGroupBox("Sort & Limit Results")
        sort_layout = QHBoxLayout()
        sort_layout.setSpacing(15)
        
        self.sort_column = QComboBox()
        self.sort_column.setMinimumWidth(150)
        self.sort_order = QComboBox()
        self.sort_order.addItems(["ASC (Ascending)", "DESC (Descending)"])
        self.sort_order.setMinimumWidth(150)
        self.limit_rows = QSpinBox()
        self.limit_rows.setRange(1, 100000)
        self.limit_rows.setValue(100)
        self.limit_rows.setMinimumWidth(80)
        
        sort_layout.addWidget(QLabel("Sort By:"))
        sort_layout.addWidget(self.sort_column)
        sort_layout.addWidget(QLabel("Order:"))
        sort_layout.addWidget(self.sort_order)
        sort_layout.addWidget(QLabel("Limit Rows:"))
        sort_layout.addWidget(self.limit_rows)
        sort_layout.addStretch()
        
        sort_group.setLayout(sort_layout)
        layout.addWidget(sort_group)
        
        # Run button
        btn_run_query = QPushButton("▶ Run Query")
        btn_run_query.clicked.connect(self.run_custom_query)
        btn_run_query.setStyleSheet("""
            QPushButton {
                background-color: #2196F3; 
                color: white; 
                font-weight: bold; 
                padding: 10px 20px;
                font-size: 12px;
            }
            QPushButton:hover {
                background-color: #1976D2;
            }
        """)
        layout.addWidget(btn_run_query)
        
        # Query result table
        self.query_result_table = QTableWidget()
        self.query_result_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.query_result_table.setAlternatingRowColors(True)
        layout.addWidget(self.query_result_table)
        
        # Result info
        self.query_info_label = QLabel("Query results will appear here")
        self.query_info_label.setStyleSheet("font-style: italic; color: #666;")
        layout.addWidget(self.query_info_label)

    def init_analytics_tab(self):
        """Initialize the Analytics tab (DuckDB integration)."""
        layout = QVBoxLayout(self.analytics_tab)
        layout.setSpacing(15)
        
        # Header
        header_label = QLabel("📊 DuckDB Analytics Module")
        header_label.setFont(QFont("Arial", 16, QFont.Weight.Bold))
        header_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(header_label)
        
        desc_label = QLabel("Sync data from SQLite to DuckDB for high-performance OLAP queries and analytics.")
        desc_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        desc_label.setStyleSheet("color: #666; font-size: 12px;")
        layout.addWidget(desc_label)
        
        # Sync controls
        sync_group = QGroupBox("Sync Controls")
        sync_layout = QHBoxLayout()
        
        btn_sync = QPushButton("🔄 Sync to DuckDB")
        btn_sync.clicked.connect(self.sync_to_duckdb)
        btn_sync.setStyleSheet("""
            QPushButton {
                background-color: #9C27B0; 
                color: white; 
                font-weight: bold; 
                padding: 10px 20px;
            }
            QPushButton:hover {
                background-color: #7B1FA2;
            }
        """)
        sync_layout.addWidget(btn_sync)
        
        btn_run_analytics = QPushButton("📈 Run Analytics Query")
        btn_run_analytics.clicked.connect(self.run_analytics_query)
        btn_run_analytics.setStyleSheet("""
            QPushButton {
                background-color: #FF9800; 
                color: white; 
                font-weight: bold; 
                padding: 10px 20px;
            }
            QPushButton:hover {
                background-color: #F57C00;
            }
        """)
        sync_layout.addWidget(btn_run_analytics)
        
        sync_layout.addStretch()
        sync_group.setLayout(sync_layout)
        layout.addWidget(sync_group)
        
        # Output area
        output_label = QLabel("Analytics Output:")
        output_label.setFont(QFont("Arial", 10, QFont.Weight.Bold))
        layout.addWidget(output_label)
        
        self.duckdb_output = QTextEdit()
        self.duckdb_output.setReadOnly(True)
        self.duckdb_output.setFont(QFont("Consolas", 10))
        self.duckdb_output.setStyleSheet("background-color: #1e1e1e; color: #d4d4d4;")
        layout.addWidget(self.duckdb_output)

    def init_menu(self):
        """Initialize the menu bar."""
        menubar = self.menuBar()
        
        # File Menu
        file_menu = menubar.addMenu("&File")
        
        open_action = QAction("&Open Database", self)
        open_action.setShortcut("Ctrl+O")
        open_action.triggered.connect(self.browse_db)
        file_menu.addAction(open_action)
        
        # Recent Files submenu
        recent_menu = file_menu.addMenu("Recent Databases")
        self.recent_file_actions = []
        self.update_recent_files_menu(recent_menu)
        
        file_menu.addSeparator()
        
        exit_action = QAction("&Exit", self)
        exit_action.setShortcut("Ctrl+Q")
        exit_action.triggered.connect(self.close_application)
        file_menu.addAction(exit_action)
        
        # Edit Menu
        edit_menu = menubar.addMenu("&Edit")
        
        clear_filters_action = QAction("&Clear Filters", self)
        clear_filters_action.setShortcut("Ctrl+L")
        clear_filters_action.triggered.connect(self.clear_filters)
        edit_menu.addAction(clear_filters_action)
        
        refresh_action = QAction("&Refresh Data", self)
        refresh_action.setShortcut("F5")
        refresh_action.triggered.connect(self.load_data_to_view)
        edit_menu.addAction(refresh_action)
        
        # Help Menu
        help_menu = menubar.addMenu("&Help")
        
        about_action = QAction("&About", self)
        about_action.triggered.connect(self.show_about)
        help_menu.addAction(about_action)
        
        docs_action = QAction("&Documentation", self)
        docs_action.setShortcut("F1")
        docs_action.triggered.connect(self.show_docs)
        help_menu.addAction(docs_action)

    def update_recent_files_menu(self, menu):
        """Update the recent files submenu."""
        menu.clear()
        for file_path in self.recent_files[-5:]:
            action = QAction(os.path.basename(file_path), self)
            action.setStatusTip(file_path)
            action.triggered.connect(lambda checked, fp=file_path: self.open_recent_file(fp))
            menu.addAction(action)
        if not self.recent_files:
            no_recent = QAction("(No recent files)", self)
            no_recent.setEnabled(False)
            menu.addAction(no_recent)

    # --- Logic Methods ---

    def browse_db(self):
        """Open file dialog to select or create a database file."""
        file_path, _ = QFileDialog.getSaveFileName(
            self, 
            "Select/Create Database", 
            "", 
            "SQLite DB (*.db);;All Files (*)"
        )
        if file_path:
            self.db_path_input.setText(file_path)
            self.add_to_recent_files(file_path)

    def browse_file(self):
        """Open file dialog to select a data file."""
        file_path, _ = QFileDialog.getOpenFileName(
            self, 
            "Select Data File", 
            "", 
            "CSV (*.csv);;Excel (*.xlsx *.xls);;All Files (*)"
        )
        if file_path:
            self.current_file = file_path
            self.file_path_input.setText(file_path)
            self.load_preview(file_path)

    def load_preview(self, file_path):
        """Load and display a preview of the data file."""
        try:
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path, nrows=5)
                self.sheet_combo.setEnabled(False)
                self.sheet_combo.clear()
                self.sheet_combo.addItem("N/A (CSV)")
            elif file_path.endswith(('.xlsx', '.xls')):
                xl = pd.ExcelFile(file_path)
                self.sheet_combo.clear()
                self.sheet_combo.addItems(xl.sheet_names)
                self.sheet_combo.setEnabled(True)
                df = pd.read_excel(xl, sheet_name=xl.sheet_names[0], nrows=5)
            else:
                QMessageBox.warning(self, "Unsupported Format", "Please select a CSV or Excel file.")
                return
            
            self.preview_table.setRowCount(len(df))
            self.preview_table.setColumnCount(len(df.columns))
            self.preview_table.setHorizontalHeaderLabels(df.columns)
            
            for i, row in df.iterrows():
                for j, val in enumerate(row):
                    item = QTableWidgetItem(str(val))
                    item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
                    self.preview_table.setItem(i, j, item)
                    
            self.preview_table.resizeColumnsToContents()
            self.preview_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Could not load preview: {str(e)}")

    def connect_db(self):
        """Connect to or create the SQLite database."""
        path = self.db_path_input.text().strip()
        if not path:
            QMessageBox.warning(self, "Missing Path", "Please select or enter a database path.")
            return
        
        try:
            # Parse table keys from filename or use default
            base_name = os.path.splitext(os.path.basename(path))[0]
            table_keys = ['timestamp']  # Default primary key
            
            # Get encryption and compression settings
            enable_encryption = self.encrypt_check.isChecked()
            compress = self.compress_check.isChecked()
            
            # Initialize the manager
            self.manager = SQLiteTimeSeriesManager(
                db_path=path,
                table_keys=table_keys,
                enable_encryption=enable_encryption,
                compress=compress
            )
            
            self.statusBar().showMessage(f"Connected to: {path}")
            QMessageBox.information(
                self, 
                "Success", 
                f"Database connected/created successfully!\n\nPath: {path}\nEncryption: {'Enabled' if enable_encryption else 'Disabled'}\nCompression: {'Enabled' if compress else 'Disabled'}"
            )
            self.update_column_combos()
            self.add_to_recent_files(path)
            
        except Exception as e:
            QMessageBox.critical(self, "Connection Error", f"Failed to connect to database:\n{str(e)}")

    def add_to_recent_files(self, file_path):
        """Add a file to the recent files list."""
        if file_path in self.recent_files:
            self.recent_files.remove(file_path)
        self.recent_files.append(file_path)
        self.save_settings()

    def open_recent_file(self, file_path):
        """Open a recently used database file."""
        if os.path.exists(file_path):
            self.db_path_input.setText(file_path)
            self.connect_db()
        else:
            QMessageBox.warning(self, "File Not Found", f"The file '{file_path}' no longer exists.")
            self.recent_files.remove(file_path)
            self.save_settings()

    def parse_type_overrides(self):
        """Parse the type override text area into a dictionary."""
        overrides = {}
        text = self.type_override_input.toPlainText().strip()
        if not text:
            return overrides
            
        for line in text.split('\n'):
            line = line.strip()
            if ':' in line and line:
                parts = line.split(':', 1)  # Split only on first colon
                if len(parts) == 2:
                    col = parts[0].strip()
                    typ = parts[1].strip().upper()
                    if col and typ:
                        overrides[col] = typ
        return overrides

    def upload_data(self):
        """Upload data from file to the database."""
        if not self.manager:
            QMessageBox.warning(self, "Not Connected", "Please connect to a database first.")
            return
        if not self.current_file or not os.path.exists(self.current_file):
            QMessageBox.warning(self, "No File", "Please select a valid data file.")
            return
            
        try:
            self.progress_bar.setVisible(True)
            self.progress_bar.setRange(0, 0)  # Indeterminate progress
            self.statusBar().showMessage("Uploading data...")
            QApplication.processEvents()
            
            # Determine sheet for Excel files
            sheet = None
            if self.current_file.endswith(('.xlsx', '.xls')) and self.sheet_combo.isEnabled():
                sheet = self.sheet_combo.currentText()
            
            # Load full data
            self.statusBar().showMessage("Loading data file...")
            QApplication.processEvents()
            
            if self.current_file.endswith('.csv'):
                df = pd.read_csv(self.current_file)
            else:
                df = pd.read_excel(self.current_file, sheet_name=sheet)
            
            # Get type overrides
            overrides = self.parse_type_overrides()
            
            # Update manager variables if overrides exist
            if overrides:
                self.manager.variables = overrides
            
            # Perform upload
            self.statusBar().showMessage("Uploading to database...")
            QApplication.processEvents()
            
            self.manager.upload_data(df)
            
            self.progress_bar.setVisible(False)
            self.statusBar().showMessage(f"Upload complete: {len(df)} rows")
            
            QMessageBox.information(
                self, 
                "Success", 
                f"Uploaded {len(df)} rows successfully!\n\nColumns: {', '.join(df.columns)}"
            )
            
            # Switch to view tab
            self.load_data_to_view()
            self.tabs.setCurrentIndex(1)
            
        except Exception as e:
            self.progress_bar.setVisible(False)
            self.statusBar().showMessage("Upload failed")
            QMessageBox.critical(self, "Upload Failed", f"Failed to upload data:\n{str(e)}")

    def update_column_combos(self):
        """Update column dropdowns with available columns from the database."""
        if self.manager:
            try:
                df = self.manager.get_all_data(limit=1)
                cols = list(df.columns)
                
                # Update filter column combo
                self.filter_column.clear()
                self.filter_column.addItems(cols)
                
                # Update sort column combo
                self.sort_column.clear()
                self.sort_column.addItems(cols)
                
            except Exception as e:
                logging.warning(f"Could not update column combos: {e}")

    def load_data_to_view(self):
        """Load all data from the database into the view table."""
        if not self.manager:
            return
        try:
            self.statusBar().showMessage("Loading data...")
            QApplication.processEvents()
            
            df = self.manager.get_all_data()
            self.populate_table(self.main_table, df)
            
            self.row_count_label.setText(f"Rows: {len(df):,}")
            self.statusBar().showMessage(f"Loaded {len(df)} rows")
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to load data:\n{str(e)}")
            self.statusBar().showMessage("Error loading data")

    def filter_table_data(self, search_text):
        """Filter the main table based on search text."""
        if not hasattr(self, 'main_table') or not search_text:
            return
        
        search_text = search_text.lower()
        for row in range(self.main_table.rowCount()):
            show_row = False
            for col in range(self.main_table.columnCount()):
                item = self.main_table.item(row, col)
                if item and search_text in item.text().lower():
                    show_row = True
                    break
            self.main_table.setRowHidden(row, not show_row)

    def populate_table(self, table_widget, df):
        """Populate a QTableWidget with DataFrame data."""
        table_widget.setRowCount(0)
        table_widget.setColumnCount(0)
        
        if df.empty:
            return
            
        table_widget.setRowCount(len(df))
        table_widget.setColumnCount(len(df.columns))
        table_widget.setHorizontalHeaderLabels(df.columns)
        
        for i, row in df.iterrows():
            for j, val in enumerate(row):
                item = QTableWidgetItem(str(val) if val is not None else "")
                # Right-align numeric values
                if isinstance(val, (int, float)):
                    item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
                table_widget.setItem(i, j, item)
        
        table_widget.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        table_widget.resizeRowsToContents()

    def add_filter_condition(self):
        """Add a filter condition to the filters list."""
        col = self.filter_column.currentText()
        op = self.filter_op.currentText()
        val = self.filter_value.text().strip()
        
        if not val:
            QMessageBox.warning(self, "Missing Value", "Please enter a filter value.")
            return
            
        row_pos = self.filters_list.rowCount()
        self.filters_list.insertRow(row_pos)
        
        # Column
        item_col = QTableWidgetItem(col)
        item_col.setFlags(item_col.flags() & ~Qt.ItemFlag.ItemIsEditable)
        self.filters_list.setItem(row_pos, 0, item_col)
        
        # Operator
        item_op = QTableWidgetItem(op)
        item_op.setFlags(item_op.flags() & ~Qt.ItemFlag.ItemIsEditable)
        self.filters_list.setItem(row_pos, 1, item_op)
        
        # Value
        item_val = QTableWidgetItem(val)
        item_val.setFlags(item_val.flags() & ~Qt.ItemFlag.ItemIsEditable)
        self.filters_list.setItem(row_pos, 2, item_val)
        
        # Delete button
        delete_btn = QPushButton("🗑️")
        delete_btn.setMaximumWidth(40)
        delete_btn.clicked.connect(lambda: self.remove_filter_row(row_pos))
        self.filters_list.setCellWidget(row_pos, 3, delete_btn)
        
        # Clear input
        self.filter_value.clear()

    def remove_filter_row(self, row):
        """Remove a filter row from the list."""
        self.filters_list.removeRow(row)

    def clear_filters(self):
        """Clear all filter conditions."""
        self.filters_list.setRowCount(0)
        self.search_input.clear()
        self.statusBar().showMessage("Filters cleared")

    def run_custom_query(self):
        """Execute a custom query based on built filters."""
        if not self.manager:
            QMessageBox.warning(self, "Not Connected", "Please connect to a database first.")
            return
            
        # Collect filters from the list
        filters = []
        for i in range(self.filters_list.rowCount()):
            col_item = self.filters_list.item(i, 0)
            op_item = self.filters_list.item(i, 1)
            val_item = self.filters_list.item(i, 2)
            
            if col_item and op_item and val_item:
                col = col_item.text()
                op = op_item.text()
                val = val_item.text()
                filters.append((col, op, val))
        
        # Get sort and limit settings
        sort_col = self.sort_column.currentText()
        sort_order_text = self.sort_order.currentText()
        sort_ord = "ASC" if "ASC" in sort_order_text else "DESC"
        limit = self.limit_rows.value()
        
        try:
            self.statusBar().showMessage("Running query...")
            QApplication.processEvents()
            
            # Execute query through manager
            df = self.manager.query_data(
                filters=filters,
                sort_by=sort_col if sort_col else None,
                sort_order=sort_ord,
                limit=limit
            )
            
            self.populate_table(self.query_result_table, df)
            
            self.query_info_label.setText(f"Query returned {len(df)} rows")
            self.statusBar().showMessage(f"Query complete: {len(df)} rows")
            
        except Exception as e:
            QMessageBox.critical(self, "Query Error", f"Failed to execute query:\n{str(e)}")
            self.statusBar().showMessage("Query failed")

    def export_data(self, fmt):
        """Export data to CSV or Excel file."""
        if not self.manager:
            QMessageBox.warning(self, "Not Connected", "Please connect to a database first.")
            return
            
        try:
            df = self.manager.get_all_data()
            
            if df.empty:
                QMessageBox.warning(self, "No Data", "No data to export.")
                return
            
            file_path, _ = QFileDialog.getSaveFileName(
                self, 
                "Export Data", 
                "", 
                f"{fmt.upper()} Files (*.{fmt});;All Files (*)"
            )
            
            if file_path:
                self.statusBar().showMessage(f"Exporting to {fmt.upper()}...")
                QApplication.processEvents()
                
                if fmt == 'csv':
                    df.to_csv(file_path, index=False)
                else:
                    df.to_excel(file_path, index=False)
                
                self.statusBar().showMessage(f"Exported to {file_path}")
                QMessageBox.information(self, "Success", f"Data exported successfully to:\n{file_path}")
                
        except Exception as e:
            QMessageBox.critical(self, "Export Error", f"Failed to export data:\n{str(e)}")
            self.statusBar().showMessage("Export failed")

    def sync_to_duckdb(self):
        """Sync data from SQLite to DuckDB for analytics."""
        if not self.manager:
            QMessageBox.warning(self, "Not Connected", "Please connect to a database first.")
            return
        
        try:
            self.duckdb_output.append("=" * 50)
            self.duckdb_output.append("Starting DuckDB sync...")
            QApplication.processEvents()
            
            # Initialize DuckDB manager if not already done
            if not self.duckdb_manager:
                duckdb_path = self.db_path_input.text().replace('.db', '_analytics.duckdb')
                self.duckdb_manager = DuckDBSyncManager(duckdb_path, self.manager)
            
            # Perform sync
            self.duckdb_output.append("Syncing data from SQLite to DuckDB...")
            QApplication.processEvents()
            
            # Placeholder for actual sync logic
            self.duckdb_manager.sync_from_sqlite()
            
            self.duckdb_output.append("✓ Sync completed successfully!")
            self.duckdb_output.append(f"Timestamp: {pd.Timestamp.now().isoformat()}")
            self.duckdb_output.append("=" * 50)
            self.duckdb_output.append("")
            
            self.statusBar().showMessage("DuckDB sync complete")
            
        except Exception as e:
            self.duckdb_output.append(f"✗ Sync failed: {str(e)}")
            QMessageBox.critical(self, "Sync Error", f"Failed to sync to DuckDB:\n{str(e)}")
            self.statusBar().showMessage("DuckDB sync failed")

    def run_analytics_query(self):
        """Run a sample analytics query on DuckDB."""
        if not self.duckdb_manager:
            QMessageBox.warning(self, "Not Synced", "Please sync to DuckDB first.")
            return
        
        try:
            self.duckdb_output.append("\n" + "=" * 50)
            self.duckdb_output.append("Running analytics query...")
            QApplication.processEvents()
            
            # Placeholder for actual analytics query
            self.duckdb_output.append("Sample Analytics Query Results:")
            self.duckdb_output.append("- Total Records: [calculated]")
            self.duckdb_output.append("- Date Range: [calculated]")
            self.duckdb_output.append("- Aggregations: [calculated]")
            self.duckdb_output.append("=" * 50)
            
            self.statusBar().showMessage("Analytics query complete")
            
        except Exception as e:
            self.duckdb_output.append(f"✗ Query failed: {str(e)}")
            QMessageBox.critical(self, "Query Error", f"Failed to run analytics query:\n{str(e)}")
            self.statusBar().showMessage("Analytics query failed")

    def show_about(self):
        """Display the about dialog."""
        QMessageBox.about(
            self, 
            "About Basilisk", 
            """<h2>🦎 Basilisk Database Manager v1.0</h2>
               <p>A powerful GUI for managing SQLite time series databases with DuckDB analytics integration.</p>
               <p><b>Features:</b></p>
               <ul>
                   <li>Automatic type inference from data</li>
                   <li>Manual type override support</li>
                   <li>Data encryption and compression</li>
                   <li>Visual query builder</li>
                   <li>CSV/Excel import and export</li>
                   <li>DuckDB analytics integration</li>
               </ul>
               <p><b>Built with:</b> PyQt6, Pandas, SQLite, DuckDB</p>
               <p>© 2026 Basilisk Team</p>"""
        )

    def show_docs(self):
        """Display documentation/help information."""
        help_dialog = QDialog(self)
        help_dialog.setWindowTitle("Documentation")
        help_dialog.setGeometry(200, 200, 600, 500)
        
        layout = QVBoxLayout(help_dialog)
        
        text_edit = QTextEdit()
        text_edit.setReadOnly(True)
        text_edit.setHtml("""
        <h1>Basilisk Database Manager - Documentation</h1>
        
        <h2>Getting Started</h2>
        <ol>
            <li><b>Create/Open Database:</b> Click "Browse..." to select or create a .db file, then click "Connect / Create Database"</li>
            <li><b>Upload Data:</b> Select a CSV or Excel file, preview the data, optionally set type overrides, and click "Upload Data"</li>
            <li><b>View Data:</b> Navigate to the "View Data" tab to see all uploaded records</li>
            <li><b>Query Data:</b> Use the "Query Builder" tab to filter, sort, and limit your data</li>
            <li><b>Analytics:</b> Sync to DuckDB for high-performance analytical queries</li>
        </ol>
        
        <h2>Type Overrides</h2>
        <p>Format: <code>column_name:TYPE</code> (one per line)</p>
        <p>Supported types: TEXT, INTEGER, REAL, BOOLEAN</p>
        <p>Example:</p>
        <pre>
        volume:INTEGER
        price:REAL
        description:TEXT
        is_active:INTEGER
        </pre>
        
        <h2>Keyboard Shortcuts</h2>
        <ul>
            <li><b>Ctrl+O:</b> Open Database</li>
            <li><b>Ctrl+Q:</b> Exit Application</li>
            <li><b>Ctrl+L:</b> Clear Filters</li>
            <li><b>F5:</b> Refresh Data</li>
            <li><b>F1:</b> Show Documentation</li>
        </ul>
        
        <h2>Tips</h2>
        <ul>
            <li>Leave type overrides empty for automatic inference</li>
            <li>Use encryption for sensitive data</li>
            <li>Enable compression after large deletions</li>
            <li>Use the search box to quickly filter visible data</li>
        </ul>
        """)
        
        layout.addWidget(text_edit)
        
        btn_close = QPushButton("Close")
        btn_close.clicked.connect(help_dialog.close)
        layout.addWidget(btn_close)
        
        help_dialog.exec()

    def close_application(self):
        """Clean up and close the application."""
        self.save_settings()
        if self.manager:
            self.manager.close()
        self.close()

    def closeEvent(self, event):
        """Handle window close event."""
        reply = QMessageBox.question(
            self,
            'Confirm Exit',
            'Are you sure you want to exit?',
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            self.close_application()
            event.accept()
        else:
            event.ignore()


def main():
    """Main entry point for the application."""
    app = QApplication(sys.argv)
    app.setApplicationName("Basilisk Database Manager")
    app.setOrganizationName("Basilisk")
    
    # Set Fusion style for consistent cross-platform appearance
    app.setStyle("Fusion")
    
    # Set application-wide font
    font = QFont("Segoe UI", 10)
    app.setFont(font)
    
    window = DatabaseManagerGUI()
    window.show()
    
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
