from PyQt5.QtWidgets import (
    QToolBar, QAction, QWidget, QHBoxLayout, QSizePolicy, QLineEdit,
    QLabel, QDialog, QVBoxLayout, QPushButton, QGridLayout, QComboBox, 
    QListWidget, QMessageBox
)
from PyQt5.QtCore import QSize, Qt, QTimer, pyqtSignal, QThread
from PyQt5.QtWidgets import QListWidgetItem

from PyQt5.QtGui import QIcon
import logging
import re
import time
from datetime import datetime

class LayoutSelectionDialog(QDialog):
    def __init__(self, parent=None, current_layout=None):
        super().__init__(parent)
        self.setWindowTitle("Select Layout")
        self.setFixedSize(400, 400)
        self.setWindowFlags(Qt.Popup)
        self.selected_layout = current_layout
        self.layout_buttons = {}
        self.initUI()

    def initUI(self):
        layout = QVBoxLayout()
        label = QLabel("Choose a layout:")
        label.setAlignment(Qt.AlignCenter)
        label.setStyleSheet("""
            QLabel {
                font-size: 16px;
                font-weight: bold;
                color: #333;
                margin-bottom: 10px;
            }
        """)
        layout.addWidget(label)
        grid = QGridLayout()
        layouts = {
            "1x2": "⬛⬛",
            "2x2": "⬛⬛\n⬛⬛",
            "3x3": "⬛⬛⬛\n⬛⬛⬛\n⬛⬛⬛"
        }
        row, col = 0, 0
        for layout_name, icon in layouts.items():
            btn = QPushButton(icon)
            btn.setFixedSize(80, 80)
            btn.setToolTip(layout_name)
            self.layout_buttons[layout_name] = btn
            btn.clicked.connect(lambda _, l=layout_name: self.select_layout(l))
            grid.addWidget(btn, row, col)
            col += 1
            if col >= 3:
                row += 1
                col = 0
        layout.addLayout(grid)
        self.setLayout(layout)
        self.update_button_styles()

    def update_button_styles(self):
        for layout_name, btn in self.layout_buttons.items():
            btn.setStyleSheet(
                "background-color: #4a90e2; color: white; font-weight: bold;"
                if layout_name == self.selected_layout
                else "background-color: #cfd8dc;"
            )

    def select_layout(self, layout):
        self.selected_layout = layout
        self.update_button_styles()
        self.accept()

class FileSelectionDialog(QDialog):
    def __init__(self, parent=None, project_name=None, model_name=None, db=None):
        super().__init__(parent)
        self.setWindowTitle("Select File to Open")
        self.setFixedSize(400, 300)
        self.project_name = project_name
        self.model_name = model_name
        self.db = db
        self.selected_file = None
        self.file_list = QListWidget()
        self.initUI()
        self.populate_files()

    def initUI(self):
        layout = QVBoxLayout()
        
        # Label
        label = QLabel("Select a file to open:")
        label.setStyleSheet("font-size: 14px; margin-bottom: 10px;")
        layout.addWidget(label)
        
        # List widget for files
        self.file_list.setStyleSheet("""
            QListWidget {
                background-color: #ffffff;
                color: #212121;
                border: 1px solid #90caf9;
                border-radius: 4px;
                padding: 4px;
                font-size: 14px;
                font-weight: 500;
            }
            QListWidget::item {
                padding: 5px;
            }
            QListWidget::item:selected {
                background-color: #4a90e2;
                color: white;
            }
        """)
        layout.addWidget(self.file_list)
        
        # Buttons
        button_layout = QHBoxLayout()
        self.open_btn = QPushButton("Open")
        self.cancel_btn = QPushButton("Cancel")
        
        self.open_btn.setStyleSheet("""
            QPushButton {
                background-color: #43a047;
                color: white;
                border: none;
                padding: 8px 16px;
                border-radius: 5px;
                font-size: 14px;
                font-weight: 500;
            }
            QPushButton:hover {
                background-color: #2e7d32;
            }
            QPushButton:pressed {
                background-color: #1b5e20;
            }
            QPushButton:disabled {
                background-color: #b0bec5;
                color: #78909c;
            }
        """)
        
        self.cancel_btn.setStyleSheet("""
            QPushButton {
                background-color: #78909c;
                color: white;
                border: none;
                padding: 8px 16px;
                border-radius: 5px;
                font-size: 14px;
                font-weight: 500;
            }
            QPushButton:hover {
                background-color: #546e7a;
            }
            QPushButton:pressed {
                background-color: #37474f;
            }
        """)
        
        self.open_btn.clicked.connect(self.accept)
        self.cancel_btn.clicked.connect(self.reject)
        self.file_list.itemDoubleClicked.connect(self.accept)
        
        button_layout.addWidget(self.open_btn)
        button_layout.addWidget(self.cancel_btn)
        layout.addLayout(button_layout)
        
        self.setLayout(layout)
        self.open_btn.setEnabled(False)
        self.file_list.itemSelectionChanged.connect(self.on_selection_changed)

    def on_selection_changed(self):
        self.open_btn.setEnabled(len(self.file_list.selectedItems()) > 0)

    def populate_files(self):
        self.file_list.clear()
        try:
            if not self.project_name or not self.model_name:
                item = QListWidgetItem("Project or model not selected")
                item.setFlags(item.flags() & ~Qt.ItemIsSelectable)
                self.file_list.addItem(item)
                self.open_btn.setEnabled(False)
                return

            if not self.db.is_connected():
                self.db.reconnect()

            filenames = self.db.get_distinct_filenames(self.project_name, self.model_name)
            
            if not filenames:
                item = QListWidgetItem("No files available")
                item.setFlags(item.flags() & ~Qt.ItemIsSelectable)
                self.file_list.addItem(item)
                self.open_btn.setEnabled(False)
            else:
                # Sort filenames numerically
                def numeric_sort_key(filename):
                    match = re.search(r'data(\d+)', filename)
                    return int(match.group(1)) if match else 0
                
                sorted_filenames = sorted(filenames, key=numeric_sort_key)
                for filename in sorted_filenames:
                    self.file_list.addItem(filename)
                self.open_btn.setEnabled(len(sorted_filenames) > 0)
        except Exception as e:
            item = QListWidgetItem("Error loading files")
            item.setFlags(item.flags() & ~Qt.ItemIsSelectable)
            self.file_list.addItem(item)
            self.open_btn.setEnabled(False)
            logging.error(f"Error populating files list: {str(e)}")

    def get_selected_file(self):
        selected_items = self.file_list.selectedItems()
        return selected_items[0].text() if selected_items else None

class SubToolBar(QWidget):
    # Signals to communicate with DashboardWindow
    start_saving_triggered = pyqtSignal()
    stop_saving_triggered = pyqtSignal()
    connect_mqtt_triggered = pyqtSignal()
    disconnect_mqtt_triggered = pyqtSignal()
    layout_selected = pyqtSignal(str)
    open_file_triggered = pyqtSignal(dict)  # Changed to emit a dict with project, model, and filename

    def __init__(self, parent):
        super().__init__(parent)
        self.parent = parent
        self.selected_layout = "2x2"
        self.filename_edit = None
        self.saving_indicator = None
        self.timer_label = None
        self.open_action = None
        self.blink_timer = QTimer(self)
        self.blink_timer.timeout.connect(self.toggle_saving_indicator)
        self.blink_state = False
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.update_timer)
        self.start_time = None
        self.current_project = None
        self.mqtt_connected = False
        self.is_saving = False
        self.initUI()
        self.parent.mqtt_status_changed.connect(self.update_mqtt_status)
        self.parent.project_changed.connect(self.update_project_status)
        self.parent.saving_state_changed.connect(self.update_saving_state)
        self.stop_saving_triggered.connect(self.schedule_files_combo_update)
        logging.debug("SubToolBar: Initialized with signal connections")

    def initUI(self):
        self.setStyleSheet("background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #eceff1, stop:1 #cfd8dc);")
        layout = QHBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)
        self.setLayout(layout)
        self.toolbar = QToolBar("Controls")
        self.toolbar.setFixedHeight(100)
        layout.addWidget(self.toolbar)
        self.update_subtoolbar()

    def toggle_saving_indicator(self):
        if self.saving_indicator:
            self.blink_state = not self.blink_state
            text = "rec 🔴" if self.blink_state else "rec ⚪"
            self.saving_indicator.setText(text)
            logging.debug(f"SubToolBar: Toggled saving indicator to {text}")

    def update_timer(self):
        if self.start_time and self.timer_label:
            elapsed = int(time.time() - self.start_time)
            hours = elapsed // 3600
            minutes = (elapsed % 3600) // 60
            seconds = elapsed % 60
            self.timer_label.setText(f"{hours:02d}:{minutes:02d}:{seconds:02d}")
            logging.debug(f"SubToolBar: Updated timer to {hours:02d}:{minutes:02d}:{seconds:02d}")

    def start_blinking(self):
        if self.is_saving and not self.blink_timer.isActive():
            self.blink_timer.start(500)
            self.start_time = time.time()
            self.timer.start(1000)
            if self.timer_label:
                self.timer_label.setText("00:00:00")
            if self.saving_indicator:
                self.saving_indicator.setText("rec 🔴")
            logging.debug("SubToolBar: Started blinking and timer")
        else:
            logging.debug(f"SubToolBar: Skipped starting blinking (is_saving={self.is_saving}, timer_active={self.blink_timer.isActive()})")

    def stop_blinking(self):
        if not self.is_saving and self.blink_timer.isActive():
            self.blink_timer.stop()
            self.timer.stop()
            if self.saving_indicator:
                self.saving_indicator.setText("")
            if self.timer_label:
                self.timer_label.setText("")
            self.start_time = None
            logging.debug("SubToolBar: Stopped blinking and timer")
        else:
            logging.debug(f"SubToolBar: Skipped stopping blinking (is_saving={self.is_saving}, timer_active={self.blink_timer.isActive()})")

    def update_saving_state(self, is_saving):
        if self.is_saving != is_saving:
            self.is_saving = is_saving
            if is_saving:
                self.start_blinking()
            else:
                self.stop_blinking()
                # Refresh dropdowns when saving stops (new data might be available)
                if hasattr(self, 'files_dropdown') and hasattr(self, 'models_dropdown'):
                    QTimer.singleShot(1000, self.refresh_dropdowns)
            self.update_subtoolbar()
            logging.debug(f"SubToolBar: Updated saving state to {is_saving}")
        else:
            logging.debug(f"SubToolBar: Saving state unchanged (is_saving={is_saving})")

    def update_mqtt_status(self, connected):
        self.mqtt_connected = connected
        self.update_subtoolbar()
        self.schedule_files_combo_update()
        logging.debug(f"SubToolBar: Updated MQTT status to {connected}")

    def update_project_status(self, project_name):
        self.current_project = project_name
        self.refresh_filename()
        self.schedule_files_combo_update()
        self.update_subtoolbar()
        # Refresh dropdowns when project changes
        if hasattr(self, 'files_dropdown') and hasattr(self, 'models_dropdown'):
            self.refresh_dropdowns()
        logging.debug(f"SubToolBar: Updated project to {project_name}")

    def schedule_files_combo_update(self):
        """Schedule an update for the files combo with a slight delay to ensure DB commit."""
        QTimer.singleShot(1000, self.update_files_list)
        logging.debug("SubToolBar: Scheduled files list update")

    def update_files_list(self):
        # This method is kept for compatibility but no longer updates a combo box
        logging.debug("SubToolBar: Files list update scheduled (no combo box to update)")

    def update_subtoolbar(self):
        logging.debug(f"SubToolBar: Updating toolbar, project: {self.current_project}, MQTT: {self.mqtt_connected}, Saving: {self.is_saving}")
        self.toolbar.clear()
        self.toolbar.setStyleSheet("""
            QToolBar { border: none; padding: 5px; spacing: 10px; }
            QToolButton { border: none; padding: 8px; border-radius: 5px; font-size: 24px; color: white; }
            QToolButton:hover { background-color: #4a90e2; }
            QToolButton:pressed { background-color: #357abd; }
            QToolButton:focus { outline: none; border: 1px solid #4a90e2; }
            QToolButton:disabled { background-color: #546e7a; color: #b0bec5; }
        """)
        self.toolbar.setIconSize(QSize(25, 25))
        self.toolbar.setMovable(False)
        self.toolbar.setFloatable(False)

        self.filename_edit = QLineEdit()
        self.filename_edit.setStyleSheet("""
            QLineEdit {
                background-color: #ffffff;
                color: #212121;
                border: 1px solid #90caf9;
                border-radius: 4px;
                padding: 4px 8px;
                font-size: 14px;
                font-weight: 500;
                min-width: 200px;
                max-width: 250px;
            }
            QLineEdit:hover { border: 1px solid #42a5f5; background-color: #f5faff; }
            QLineEdit:focus { border: 1px solid #1e88e5; background-color: #ffffff; }
            QLineEdit[readOnly="true"] { background-color: #e0e0e0; color: #616161; border: 1px solid #b0bec5; }
        """)
        self.filename_edit.setEnabled(self.current_project is not None)
        self.refresh_filename()
        self.toolbar.addWidget(self.filename_edit)

        self.saving_indicator = QLabel("")
        self.saving_indicator.setStyleSheet("font-size: 20px; padding: 0px 8px;")
        self.toolbar.addWidget(self.saving_indicator)

        self.timer_label = QLabel("")
        self.timer_label.setStyleSheet("font-size: 20px; padding: 0px 8px;")
        self.toolbar.addWidget(self.timer_label)

        if self.is_saving:
            self.start_blinking()
        else:
            self.stop_blinking()

        def add_action(text_icon, color, callback, tooltip, enabled, background_color):
            action = QAction(text_icon, self)
            action.triggered.connect(callback)
            action.setToolTip(tooltip)
            action.setEnabled(enabled)
            self.toolbar.addAction(action)
            button = self.toolbar.widgetForAction(action)
            if button:
                button.setStyleSheet(f"""
                    QToolButton {{
                        color: {color};
                        font-size: 24px;
                        border: none;
                        padding: 8px;
                        border-radius: 5px;
                        background-color: {background_color if enabled else '#546e7a'};
                    }}
                    QToolButton:hover {{ background-color: #4a90e2; }}
                    QToolButton:pressed {{ background-color: #357abd; }}
                    QToolButton:disabled {{ background-color: #546e7a; color: #b0bec5; }}
                """)

        add_action("▶", "#ffffff", self.start_saving_triggered, "Start Saving Data", not self.is_saving and self.current_project is not None, "#43a047")
        add_action("⏸", "#ffffff", self.stop_saving_triggered, "Stop Saving Data", self.is_saving, "#d8291d")
        self.toolbar.addSeparator()

        # Add saved files dropdown
        self.files_dropdown = QComboBox()
        self.files_dropdown.setToolTip("Select Saved File")
        self.files_dropdown.setMinimumWidth(150)
        self.files_dropdown.setMinimumHeight(30)
        self.files_dropdown.setStyleSheet("""
            QComboBox {
                color: #ffffff;
                background-color: #546e7a;
                border: 1px solid #37474f;
                border-radius: 5px;
                padding: 8px;
                font-size: 14px;
                min-height: 30px;
            }
            QComboBox:hover {
                background-color: #607d8b;
            }
            QComboBox::drop-down {
                border: none;
                width: 25px;
                min-height: 30px;
            }
            QComboBox::down-arrow {
                image: none;
                border-left: 5px solid transparent;
                border-right: 5px solid transparent;
                border-top: 5px solid #ffffff;
                margin-right: 8px;
            }
            QComboBox QAbstractItemView {
                color: #000000;
                background-color: #ffffff;
                selection-background-color: #4a90e2;
                min-height: 150px;
            }
        """)
        self.toolbar.addWidget(self.files_dropdown)

        # Add models dropdown
        self.models_dropdown = QComboBox()
        self.models_dropdown.setToolTip("Select Model")
        self.models_dropdown.setMinimumWidth(150)
        self.models_dropdown.setMinimumHeight(30)
        self.models_dropdown.setStyleSheet("""
            QComboBox {
                color: #ffffff;
                background-color: #546e7a;
                border: 1px solid #37474f;
                border-radius: 5px;
                padding: 8px;
                font-size: 14px;
                min-height: 30px;
            }
            QComboBox:hover {
                background-color: #607d8b;
            }
            QComboBox::drop-down {
                border: none;
                width: 25px;
                min-height: 30px;
            }
            QComboBox::down-arrow {
                image: none;
                border-left: 5px solid transparent;
                border-right: 5px solid transparent;
                border-top: 5px solid #ffffff;
                margin-right: 8px;
            }
            QComboBox QAbstractItemView {
                color: #000000;
                background-color: #ffffff;
                selection-background-color: #4a90e2;
                min-height: 150px;
            }
        """)
        self.toolbar.addWidget(self.models_dropdown)

        # Add open button
        self.open_dropdown_action = QAction("open", self)
        self.open_dropdown_action.setToolTip("Open Frequency Plot")
        self.open_dropdown_action.triggered.connect(self.open_frequency_plot)
        self.open_dropdown_action.setEnabled(self.current_project is not None)
        self.toolbar.addAction(self.open_dropdown_action)
        open_dropdown_button = self.toolbar.widgetForAction(self.open_dropdown_action)
        if open_dropdown_button:
            open_dropdown_button.setStyleSheet(f"""
                QToolButton {{
                    color: #ffffff;
                    font-size: 25px;
                    border: none;
                    padding: 6px;
                    border-radius: 5px;
                    background-color: {'#43a047' if self.open_dropdown_action.isEnabled() else '#546e7a'};
                }}
                QToolButton:hover {{ background-color: #4a90e2; }}
                QToolButton:pressed {{ background-color: #357abd; }}
                QToolButton:disabled {{ background-color: #546e7a; color: #b0bec5; }}
            """)

        # Populate dropdowns
        self.refresh_dropdowns()

        # Keep the old open action for compatibility (but hidden)
        self.open_action = QAction("saved files", self)
        self.open_action.setToolTip("Open File")
        self.open_action.triggered.connect(self.open_selected_file)
        self.open_action.setEnabled(False)  # Disable the old one
        self.open_action.setVisible(False)  # Hide the old one
        self.toolbar.addAction(self.open_action)

        # Add spacer to push MQTT buttons to the right
        spacer = QWidget()
        spacer.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        self.toolbar.addWidget(spacer)

        # Add MQTT buttons before layout button
        self.toolbar.addSeparator()
        
        connect_enabled = not self.mqtt_connected
        disconnect_enabled = self.mqtt_connected
        connect_bg = "#43a047" if connect_enabled else "#546e7a"
        disconnect_bg = "#ef5350" if disconnect_enabled else "#546e7a"
        add_action("🔗", "#ffffff", self.connect_mqtt_triggered, "Connect to MQTT", connect_enabled, connect_bg)
        add_action("🔌", "#ffffff", self.disconnect_mqtt_triggered, "Disconnect from MQTT", disconnect_enabled, disconnect_bg)

        # Layout button at the right end
        layout_action = QAction("🖼️", self)
        layout_action.setToolTip("Select Layout")
        layout_action.triggered.connect(self.show_layout_menu)
        self.toolbar.addAction(layout_action)
        layout_button = self.toolbar.widgetForAction(layout_action)
        if layout_button:
            layout_button.setStyleSheet("""
                QToolButton {
                    color: #ffffff;
                    font-size: 24px;
                    border: none;
                    padding: 8px;
                    border-radius: 5px;
                }
                QToolButton:hover { background-color: #4a90e2; }
                QToolButton:pressed { background-color: #357abd; }
            """)
        
        self.toolbar.repaint()

    def refresh_dropdowns(self):
        """Refresh the dropdowns with current project data"""
        try:
            # Clear current items
            self.files_dropdown.clear()
            self.models_dropdown.clear()
            
            if not self.current_project:
                self.files_dropdown.addItem("No project selected")
                self.models_dropdown.addItem("No project selected")
                self.open_dropdown_action.setEnabled(False)
                return
            
            # Get models for the current project
            project_data = self.parent.db.get_project_data(self.current_project)
            if project_data and "models" in project_data:
                models = [model["name"] for model in project_data["models"]]
                self.models_dropdown.addItems(models)
                
                # Enable/disable based on availability
                if models:
                    self.models_dropdown.setEnabled(True)
                    # Load files for the first model
                    self.refresh_files_for_model(models[0])
                else:
                    self.models_dropdown.addItem("No models found")
                    self.models_dropdown.setEnabled(False)
                    self.files_dropdown.addItem("No models found")
                    self.open_dropdown_action.setEnabled(False)
            else:
                self.models_dropdown.addItem("No models found")
                self.models_dropdown.setEnabled(False)
                self.files_dropdown.addItem("No models found")
                self.open_dropdown_action.setEnabled(False)
            
            # Connect model selection change to refresh files
            self.models_dropdown.currentTextChanged.connect(self.refresh_files_for_model)
            
        except Exception as e:
            logging.error(f"Error refreshing dropdowns: {str(e)}")
            self.files_dropdown.addItem("Error loading data")
            self.models_dropdown.addItem("Error loading data")
            self.open_dropdown_action.setEnabled(False)

    def refresh_files_for_model(self, model_name):
        """Refresh files dropdown based on selected model"""
        try:
            self.files_dropdown.clear()
            
            if not model_name or model_name == "No models found" or not self.current_project:
                self.files_dropdown.addItem("No model selected")
                self.open_dropdown_action.setEnabled(False)
                return
            
            # Get files for the selected model
            filenames = self.parent.db.get_distinct_filenames(self.current_project, model_name)

            if filenames:
                # Show human-friendly labels: "dataN start ... -- stop ..." but keep raw filename as item data
                for fname in filenames:
                    label = self._format_saved_file_label(fname, model_name)
                    index = self.files_dropdown.count()
                    self.files_dropdown.addItem(label)
                    # Store the raw filename so we can open the file without the extra text
                    self.files_dropdown.setItemData(index, fname)
                self.open_dropdown_action.setEnabled(True)
            else:
                self.files_dropdown.addItem("No files found")
                self.open_dropdown_action.setEnabled(False)
                
        except Exception as e:
            logging.error(f"Error refreshing files for model {model_name}: {str(e)}")
            self.files_dropdown.addItem("Error loading files")
            self.open_dropdown_action.setEnabled(False)

    def _format_saved_file_label(self, filename, model_name):
        """Return a display label like
        "data1 start 10-01-2025 12:07:01 -- stop 10-01-2025 12:08:02"
        for the given filename, if timestamps are available.
        """
        try:
            if not self.current_project or not getattr(self.parent, "db", None):
                return filename

            # Query only first and last message to avoid loading large datasets
            col = self.parent.db.history_collection
            base_query = {
                "projectName": self.current_project,
                "email": self.parent.email,
                "moduleName": model_name,
                "filename": filename,
            }
            start_doc = col.find(base_query, {"createdAt": 1}).sort("createdAt", 1).limit(1)
            stop_doc = col.find(base_query, {"createdAt": 1}).sort("createdAt", -1).limit(1)
            start_ts = None
            stop_ts = None
            try:
                start_ts = next(start_doc, {}).get("createdAt")
            except Exception:
                pass
            try:
                stop_ts = next(stop_doc, {}).get("createdAt")
            except Exception:
                pass
            if not start_ts and not stop_ts:
                return filename

            def _fmt(ts_val):
                if isinstance(ts_val, datetime):
                    dt = ts_val
                elif isinstance(ts_val, str):
                    # Try common ISO formats; fall back to raw string on failure
                    try:
                        # Handle trailing Z (UTC) if present
                        cleaned = ts_val.replace("Z", "+00:00")
                        dt = datetime.fromisoformat(cleaned)
                    except Exception:
                        return str(ts_val)
                else:
                    return str(ts_val)
                return dt.strftime("%d-%m-%Y %H:%M:%S")

            start_str = _fmt(start_ts)
            stop_str = _fmt(stop_ts)

            return f"{filename} start {start_str} -- stop {stop_str}"
        except Exception as e:
            logging.error(f"SubToolBar: Error formatting label for {filename}: {e}")
            return filename

    def open_frequency_plot(self):
        """Open frequency plot with selected file and model"""
        try:
            # Use the raw filename stored as item data, fall back to text if missing
            selected_file = self.files_dropdown.currentData()
            if not selected_file:
                selected_file = self.files_dropdown.currentText()
            selected_model = self.models_dropdown.currentText()
            
            if not self.current_project:
                QMessageBox.warning(self, "Error", "No project selected!")
                return
            
            if not selected_model or selected_model in ["No models found", "No model selected"]:
                QMessageBox.warning(self, "Error", "Please select a model!")
                return
            
            if not selected_file or selected_file in ["No files found", "No model selected", "Error loading files"]:
                QMessageBox.warning(self, "Error", "Please select a valid file!")
                return
            
            # Create file data dict similar to the original open_selected_file
            file_data = {
                "project_name": self.current_project,
                "model_name": selected_model,
                "filename": selected_file
            }
            
            # Emit the signal to open the frequency plot
            self.open_file_triggered.emit(file_data)
            logging.debug(f"SubToolBar: Open frequency plot triggered for {file_data}")
            
        except Exception as e:
            logging.error(f"Error opening frequency plot: {str(e)}")
            QMessageBox.warning(self, "Error", f"Failed to open frequency plot: {str(e)}")

    def open_selected_file(self):
        # Show file selection dialog
        model_name = self.parent.tree_view.get_selected_model()
        if not model_name:
            QMessageBox.warning(self, "Error", "Please select a model first!")
            return
            
        if not self.current_project:
            QMessageBox.warning(self, "Error", "No project selected!")
            return

        dialog = FileSelectionDialog(
            parent=self,
            project_name=self.current_project,
            model_name=model_name,
            db=self.parent.db
        )
        
        # Position dialog in center of parent
        parent_geom = self.parent.geometry()
        dialog.move(
            parent_geom.x() + (parent_geom.width() - dialog.width()) // 2,
            parent_geom.y() + (parent_geom.height() - dialog.height()) // 2
        )
        
        if dialog.exec_() == QDialog.Accepted:
            selected_file = dialog.get_selected_file()
            if selected_file and selected_file not in ["No files available", "Project or model not selected", "Error loading files"]:
                if model_name and self.current_project:
                    file_data = {
                        "project_name": self.current_project,
                        "model_name": model_name,
                        "filename": selected_file
                    }
                    self.open_file_triggered.emit(file_data)
                    logging.debug(f"SubToolBar: Open file triggered for {file_data}")
                else:
                    logging.debug(f"SubToolBar: Cannot open file, missing model or project: model={model_name}, project={self.current_project}")
            else:
                logging.debug(f"SubToolBar: Invalid file selection: {selected_file}")
        else:
            logging.debug("SubToolBar: File selection dialog cancelled")

    def refresh_filename(self):
        if not self.filename_edit:
            return
        try:
            next_filename = "data1"
            filename_counter = 1
            if self.current_project:
                model_name = self.parent.tree_view.get_selected_model()
                if model_name:
                    filenames = self.parent.db.get_distinct_filenames(self.current_project, model_name)
                else:
                    # Fall back to all files in the project so numbering continues even without a model selection
                    filenames = self.parent.db.get_distinct_filenames(self.current_project)

                if filenames:
                    # Extract numbers from filenames and find the next available number
                    numbers = []
                    for f in filenames:
                        match = re.match(r"data(\d+)", f)
                        if match:
                            numbers.append(int(match.group(1)))
                    filename_counter = max(numbers, default=0) + 1
                elif not model_name:
                    logging.debug("SubToolBar: No filenames found for project; defaulting to data1")

                next_filename = f"data{filename_counter}"
            self.filename_edit.setText(next_filename)
            logging.debug(f"SubToolBar: Refreshed filename to {next_filename}")
        except Exception as e:
            logging.error(f"SubToolBar: Error refreshing filename: {str(e)}")
            self.filename_edit.setText("data1")

    def show_layout_menu(self):
        dialog = LayoutSelectionDialog(self, current_layout=self.selected_layout)
        parent_geom = self.parent.geometry()
        dialog.move(
            parent_geom.x() + (parent_geom.width() - dialog.width()) // 2,
            parent_geom.y() + (parent_geom.height() - dialog.height()) // 2
        )
        if dialog.exec_() == QDialog.Accepted:
            self.layout_selected.emit(dialog.selected_layout)