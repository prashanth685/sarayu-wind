import numpy as np
from PyQt5.QtWidgets import QWidget, QVBoxLayout, QTableWidget, QTableWidgetItem, QScrollArea, QPushButton, QCheckBox, QComboBox, QHBoxLayout, QGridLayout, QLabel, QSizePolicy, QHeaderView
from PyQt5.QtCore import Qt, QThread, QObject, pyqtSignal, QTimer, QPropertyAnimation, QEasingCurve
from PyQt5.QtGui import QIcon, QFont
import pyqtgraph as pg
from datetime import datetime
import scipy.signal as signal
import logging
import sip

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class TabularViewSettings:
    def __init__(self, project_id):
        self.project_id = project_id
        self.channel_name_visible = True
        self.unit_visible = True
        self.datetime_visible = True
        self.rpm_visible = True
        self.gap_visible = True
        self.direct_visible = True
        self.bandpass_visible = True
        self.one_xa_visible = True
        self.one_xp_visible = True
        self.two_xa_visible = True
        self.two_xp_visible = True
        self.nx_amp_visible = True
        self.nx_phase_visible = True
        self.updated_at = datetime.utcnow()

class TabularViewWorker(QObject):
    finished = pyqtSignal()
    error = pyqtSignal(str)
    initialized = pyqtSignal(list, int, str, dict, str)

    def __init__(self, parent, project_name, model_name, db):
        super().__init__()
        self.parent = parent
        self.project_name = project_name
        self.model_name = model_name
        self.db = db

    def run(self):
        try:
            database = self.db.client.get_database("changed_db")
            projects_collection = database.get_collection("projects")
            project = projects_collection.find_one({"project_name": self.project_name, "email": self.db.email})
            if not project:
                self.error.emit(f"Project {self.project_name} not found for email {self.db.email}.")
                self.initialized.emit(["Channel 1"], 1, "", {}, None)
                return
            project_id = project["_id"]
            model = next((m for m in project["models"] if m["name"] == self.model_name), None)
            if not model or not model.get("channels"):
                self.error.emit(f"Model {self.model_name} or channels not found in project {self.project_name}.")
                self.initialized.emit(["Channel 1"], 1, "", {}, None)
                return
            channel_names = [c.get("channelName", f"Channel {i+1}") for i, c in enumerate(model["channels"])]
            num_channels = len(channel_names)
            if not channel_names:
                self.error.emit("No channels found in model.")
                self.initialized.emit(["Channel 1"], 1, "", {}, None)
                return
            channel_properties = {}
            for channel in model["channels"]:
                channel_name = channel.get("channelName", "Unknown")
                correction_value = float(channel.get("correctionValue", "1.0")) if channel.get("correctionValue") else 1.0
                gain = float(channel.get("gain", "1.0")) if channel.get("gain") else 1.0
                sensitivity = float(channel.get("sensitivity", "1.0")) if channel.get("sensitivity") and float(channel.get("sensitivity")) != 0 else 1.0
                unit = channel.get("unit", "mil").lower().strip()
                channel_properties[channel_name] = {
                    "Unit": unit,
                    "CorrectionValue": correction_value,
                    "Gain": gain,
                    "Sensitivity": sensitivity
                }
            tag_name = model.get("tagName", "")
            logging.debug(f"Worker initialized: {num_channels} channels, names: {channel_names}")
            # Emit project_id as string to match signal signature and avoid ObjectId type errors
            self.initialized.emit(channel_names, num_channels, tag_name, channel_properties, str(project_id))
        except Exception as ex:
            self.error.emit(f"Error initializing TabularView: {str(ex)}")
            self.initialized.emit(["Channel 1"], 1, "", {}, None)
        finally:
            self.finished.emit()

class TabularViewFeature:
    def __init__(self, parent, db, project_name, channel=None, model_name=None, console=None):
        self.parent = parent
        self.db = db
        self.project_name = project_name
        self.model_name = model_name
        self.console = console
        self.widget = None
        self.data = None
        self.sample_rate = 4096
        self.num_channels = 1
        self.channel_names = ["Channel 1"]
        self.channel_properties = {}
        self.project_id = None
        self.selected_channel = 0  # Fixed to Channel 1
        self.raw_data = [np.zeros(4096)]
        self.low_pass_data = [np.zeros(4096)]
        self.high_pass_data = [np.zeros(4096)]
        self.band_pass_data = [np.zeros(4096)]
        self.time_points = np.arange(4096) / self.sample_rate
        self.band_pass_peak_to_peak_history = [[]]
        self.band_pass_peak_to_peak_times = [[]]
        self.average_frequency = [0.0]
        self.band_pass_peak_to_peak = [0.0]
        self.one_x_amps = [[]]
        self.one_x_phases = [[]]
        self.two_x_amps = [[]]
        self.two_x_phases = [[]]
        self.three_x_amps = [[]]
        self.three_x_phases = [[]]
        self.start_time = datetime.now()
        # Use keys that match the table header labels for consistency
        self.column_visibility = {
            "Channel Name": True, "Unit": True, "DateTime": True, "RPM": True, "Gap": True,
            "Direct": True, "Bandpass": True, "1xAmp": True, "1xPhase": True, "2xAmp": True,
            "2xPhase": True, "NXAmp": True, "NXPhase": True
        }
        self.plot_initialized = False
        self.table = None
        self.plot_widgets = []
        self.plots = []
        self.plots_enabled = False  # Tabular view: disable graph plotting per user request
        self.tag_name = ""
        self.scroll_content = None
        self.scroll_layout = None
        self.mongo_client = self.db.client
        self.timer = QTimer()
        self.timer.timeout.connect(self.update_display)
        self.timer.start(1000)
        self.table_initialized = False
        self.data_buffer = []  # Buffer for incoming data
        self.last_update_time = datetime.now()
        self.update_interval = 0.5  # Update every 0.5 seconds
        # Calibration constants to match Time View
        self.scaling_factor = 3.3 / 65535.0
        self.off_set = 32768
        # Latest gap voltages from header[15..28] scaled by 1/100 (signed int16)
        self.gap_voltages = []
        # Headers: keep stable internal keys and customizable display labels for NX columns
        self.internal_headers = [
            "Channel Name", "Unit", "DateTime", "RPM", "Gap", "Direct", "Bandpass",
            "1xAmp", "1xPhase", "2xAmp", "2xPhase", "NXAmp", "NXPhase"
        ]
        self.custom_nx_amp_header = "NXAmp"
        self.custom_nx_phase_header = "NXPhase"
        self.initUI()
        self.initialize_thread()

    def get_display_headers(self):
        """Return header labels for display, with customizable NX headers."""
        headers = list(self.internal_headers)
        # Replace the last two with custom labels while keeping order
        try:
            nx_amp_index = headers.index("NXAmp")
            headers[nx_amp_index] = self.custom_nx_amp_header or "NXAmp"
        except ValueError:
            pass
        try:
            nx_phase_index = headers.index("NXPhase")
            headers[nx_phase_index] = self.custom_nx_phase_header or "NXPhase"
        except ValueError:
            pass
        return headers

    def apply_custom_headers(self):
        """Apply current custom NX headers to the table and settings checkboxes UI."""
        try:
            if self.table:
                self.table.setHorizontalHeaderLabels(self.get_display_headers())
            # Update checkbox labels but keep internal keys in the dict
            try:
                if "NXAmp" in self.checkbox_dict and self.checkbox_dict["NXAmp"]:
                    self.checkbox_dict["NXAmp"].setText(self.custom_nx_amp_header or "NXAmp")
            except Exception:
                pass
            try:
                if "NXPhase" in self.checkbox_dict and self.checkbox_dict["NXPhase"]:
                    self.checkbox_dict["NXPhase"].setText(self.custom_nx_phase_header or "NXPhase")
            except Exception:
                pass
        except Exception:
            pass

    def initUI(self):
        pg.setConfigOption('background', 'w')
        pg.setConfigOption('foreground', 'k')
        self.widget = QWidget()
        layout = QVBoxLayout()
        self.widget.setLayout(layout)

        # Top bar: header (center) and settings button (right) on the same line
        top_layout = QHBoxLayout()
        header = QLabel(f"TABULAR VIEW")
        header.setStyleSheet("color: black; font-size: 30px; font-weight: bold; padding: 8px;font-family: 'Times New Roman', Times, serif;")
        top_layout.addStretch()  # left spacer
        top_layout.addWidget(header, alignment=Qt.AlignCenter)
        top_layout.addStretch()  # right spacer to keep header centered
        self.settings_button = QPushButton("⚙️ Settings")
        self.settings_button.setStyleSheet("""
            QPushButton {
                background-color: #4CAF50;
                color: white;
                border: none;
                padding: 8px 16px;
                border-radius: 4px;
                font-size: 14px;
                min-width: 120px;
            }
            QPushButton:hover {
                background-color: #45a049;
            }
            QPushButton:pressed {
                background-color: #3d8b40;
            }
        """)
        self.settings_button.clicked.connect(self.toggle_settings)
        top_layout.addWidget(self.settings_button)
        layout.addLayout(top_layout)

        # Right sidebar settings panel (hidden by default)
        self.settings_panel = QWidget()
        self.settings_panel.setStyleSheet("""
            QWidget {
                background-color: #f5f5f5;
                border: 1px solid #d0d0d0;
                border-radius: 4px;
                padding: 10px;
            }
            QLabel#settingsTitle { font-size: 16px; font-weight: 700; padding: 4px 0 10px 0; }
        """)
        self.settings_panel.setVisible(False)
        # Target width for the sliding panel
        self._settings_width = 350
        self.settings_panel.setFixedWidth(self._settings_width)
        # Allow collapse by relaxing the minimum width
        self.settings_panel.setMinimumWidth(0)
        # For slide animation, control maximumWidth
        self.settings_panel.setMaximumWidth(0)
        settings_layout = QGridLayout()
        settings_layout.setSpacing(10)
        # Make columns proportionally flexible
        settings_layout.setColumnStretch(0, 1)
        settings_layout.setColumnStretch(1, 1)
        settings_layout.setColumnStretch(2, 1)
        self.settings_panel.setLayout(settings_layout)

        title = QLabel("Tabular View Settings")
        title.setObjectName("settingsTitle")
        settings_layout.addWidget(title, 0, 0, 1, 3)

        # Custom headers for NX columns
        nx_amp_label = QLabel("NX Amp Header:")
        self.nx_amp_header_input = QComboBox()
        # Use editable combo to allow typing or choosing defaults
        self.nx_amp_header_input.setEditable(True)
        self.nx_amp_header_input.addItems([self.custom_nx_amp_header, "NXAmp", "3xAmp", "Harmonic Amp"])  # some suggestions
        self.nx_amp_header_input.setCurrentText(self.custom_nx_amp_header)
        settings_layout.addWidget(nx_amp_label, 1, 0)
        settings_layout.addWidget(self.nx_amp_header_input, 1, 1, 1, 2)

        nx_phase_label = QLabel("NX Phase Header:")
        self.nx_phase_header_input = QComboBox()
        self.nx_phase_header_input.setEditable(True)
        self.nx_phase_header_input.addItems([self.custom_nx_phase_header, "NXPhase", "3xPhase", "Harmonic Phase"])  # suggestions
        self.nx_phase_header_input.setCurrentText(self.custom_nx_phase_header)
        settings_layout.addWidget(nx_phase_label, 2, 0)
        settings_layout.addWidget(self.nx_phase_header_input, 2, 1, 1, 2)

        # Use internal headers for visibility keys; display text for NX columns will be customized
        headers = list(self.internal_headers)
        self.checkbox_dict = {}
        # Create a scrollable area for many checkboxes to avoid overflow
        opts_scroll = QScrollArea()
        opts_scroll.setWidgetResizable(True)
        opts_scroll.setStyleSheet("QScrollArea { border: none; }")
        opts_container = QWidget()
        opts_layout = QVBoxLayout(opts_container)
        opts_layout.setContentsMargins(0, 0, 0, 0)
        opts_layout.setSpacing(6)
        for header in headers:
            # Create checkboxes for visibility; for NX columns use current custom labels but key them by internal name
            display_text = header
            key = header
            if header == "NXAmp":
                display_text = self.custom_nx_amp_header or header
            elif header == "NXPhase":
                display_text = self.custom_nx_phase_header or header
            cb = QCheckBox(display_text)
            cb.setChecked(self.column_visibility.get(key, True))
            cb.setStyleSheet("font-size: 14px;")
            # Immediate apply on toggle
            cb.toggled.connect(lambda checked, h=key: self.on_column_toggle(h, checked))
            self.checkbox_dict[key] = cb
            opts_layout.addWidget(cb)
        opts_layout.addStretch()
        opts_scroll.setWidget(opts_container)
        # Place the options scroll area in row 3 spanning full width of the panel
        settings_layout.addWidget(opts_scroll, 3, 0, 1, 3)

        self.save_settings_button = QPushButton("Save")
        self.save_settings_button.setStyleSheet("""
            QPushButton {
                background-color: #2196F3;
                color: white;
                border: none;
                padding: 8px 16px;
                border-radius: 4px;
                font-size: 14px;
                min-width: 100px;
            }
            QPushButton:hover {
                background-color: #1e88e5;
            }
            QPushButton:pressed {
                background-color: #1976d2;
            }
        """)
        self.save_settings_button.clicked.connect(self.save_settings)

        self.close_settings_button = QPushButton("Close")
        self.close_settings_button.setStyleSheet("""
            QPushButton {
                background-color: #f44336;
                color: white;
                border: none;
                padding: 8px 16px;
                border-radius: 4px;
                font-size: 14px;
                min-width: 100px;
            }
            QPushButton:hover {
                background-color: #e53935;
            }
            QPushButton:pressed {
                background-color: #d32f2f;
            }
        """)
        self.close_settings_button.clicked.connect(self.close_settings)

        # Push buttons to the bottom below the scroll area
        buttons_row = 4
        settings_layout.setRowStretch(3, 1)  # make the scroll area take remaining space
        settings_layout.addWidget(self.save_settings_button, buttons_row, 0, alignment=Qt.AlignRight)
        settings_layout.addWidget(self.close_settings_button, buttons_row, 1, alignment=Qt.AlignLeft)
        settings_layout.addWidget(QLabel(""), buttons_row, 2)

        # Left content: table + plots scroll area
        self.table = QTableWidget()
        self.table.setColumnCount(len(self.internal_headers))
        self.table.setHorizontalHeaderLabels(self.get_display_headers())
        # Table styling and behavior
        self.table.setAlternatingRowColors(True)
        self.table.setShowGrid(True)
        self.table.setStyleSheet("""
            QTableWidget {
                background: #ffffff;
                alternate-background-color: #f7f9fc; /* light color for alternate rows */
                gridline-color: #e0e6ef;
            }
            QTableWidget::item {
                padding: 6px 10px; /* increase padding */
            }
            QHeaderView::section {
                background-color: #2196F3; /* blue */
                color: #ffffff; /* white font */
                padding: 8px 10px;
                border: 1px solid #1976d2; /* darker blue border */
                font-weight: 700;
                font-size: 15px;
            }
        """)
        # Remove internal scrollbars; we will auto-adjust height
        self.table.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.table.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        # Header resize behavior
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.table.verticalHeader().setVisible(False)
        self.table.verticalHeader().setDefaultSectionSize(32)  # increase row height
        # Size policy: expand horizontally, fixed vertically (we control height)
        self.table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        # Do not add table directly to main layout; add to left container later

        self.table_initialized = True
        if self.console:
            self.console.append_to_console(f"Initialized table with {self.num_channels} rows for channels: {self.channel_names}")

        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_content = QWidget()
        self.scroll_layout = QVBoxLayout(self.scroll_content)
        self.scroll_area.setWidget(self.scroll_content)
        # Disable plots in Tabular View per request
        self.scroll_area.setVisible(False)

        # Compose left container
        left_container = QWidget()
        left_layout = QVBoxLayout(left_container)
        left_layout.setContentsMargins(0, 0, 0, 0)
        left_layout.setSpacing(8)
        left_layout.addWidget(self.table)
        left_layout.addWidget(self.scroll_area)

        # Content area: left content + right settings panel
        content_layout = QHBoxLayout()
        content_layout.setContentsMargins(0, 0, 0, 0)
        content_layout.setSpacing(8)
        content_layout.addWidget(left_container, 1)
        content_layout.addWidget(self.settings_panel)
        # Prepare animation for sliding the settings panel
        self._settings_anim = QPropertyAnimation(self.settings_panel, b"maximumWidth")
        self._settings_anim.setDuration(200)
        self._settings_anim.setEasingCurve(QEasingCurve.InOutCubic)
        layout.addLayout(content_layout)

        # Do not initialize plots (disabled for Tabular View)
        if self.console:
            self.console.append_to_console(f"Initialized UI with channel: {self.channel_names[self.selected_channel]}")

    def initialize_thread(self):
        self.worker = TabularViewWorker(self, self.project_name, self.model_name, self.db)
        self.thread = QThread()
        self.worker.moveToThread(self.thread)
        self.thread.started.connect(self.worker.run)
        self.worker.finished.connect(self.thread.quit)
        self.worker.finished.connect(self.worker.deleteLater)
        self.thread.finished.connect(self.thread.deleteLater)
        self.worker.error.connect(self.log_and_set_status)
        self.worker.initialized.connect(self.complete_initialization)
        self.thread.start()

    def complete_initialization(self, channel_names, num_channels, tag_name, channel_properties, project_id):
        try:
            self.channel_names = channel_names
            self.num_channels = num_channels
            self.tag_name = tag_name
            self.channel_properties = channel_properties
            self.project_id = project_id
            self.table.setRowCount(self.num_channels)
            self.initialize_data_arrays()
            self.update_table_defaults()
            self.load_settings_from_database()
            self.initialize_plots()
            if self.console:
                self.console.append_to_console(f"Completed initialization: TagName: {self.tag_name}, Model: {self.model_name}, Channels: {self.num_channels}, Names: {self.channel_names}")
        except Exception as ex:
            self.log_and_set_status(f"Error completing initialization: {str(ex)}")
            self.channel_names = ["Channel 1"]
            self.num_channels = 1
            self.table.setRowCount(1)
            self.initialize_data_arrays()
            self.update_table_defaults()
            self.initialize_plots()

    def initialize_plots(self):
        if not self.plots_enabled:
            # Plots are disabled in Tabular View
            self.plot_initialized = False
            return
        for widget in self.plot_widgets:
            self.scroll_layout.removeWidget(widget)
            widget.deleteLater()
        self.plot_widgets = []
        self.plots = []

        plot_titles = [
            "Raw Data", "Low-Pass Filtered Data (20 Hz)", "High-Pass Filtered Data (200 Hz)",
            "Band-Pass Filtered Data (50-200 Hz)", "Bandpass Peak-to-Peak Over Time"
        ]
        for title in plot_titles:
            plot_widget = pg.PlotWidget(title=title)
            plot_widget.showGrid(x=True, y=True)
            plot_widget.setLabel('bottom', 'Time (s)' if title != "Bandpass Peak-to-Peak Over Time" else 'Time (s)')
            unit_label = self.get_unit_label()
            plot_widget.setLabel('left', f'Amplitude ({unit_label})' if title != "Bandpass Peak-to-Peak Over Time" else f'Peak-to-Peak Value ({unit_label})')
            # Responsive sizing for plots
            plot_widget.setMinimumHeight(180)
            plot_widget.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
            self.scroll_layout.addWidget(plot_widget)
            self.plot_widgets.append(plot_widget)
            plot = plot_widget.plot(pen='b')
            self.plots.append(plot)
        self.scroll_layout.addStretch()
        self.plot_initialized = True
        self.update_plots()

    def get_unit_label(self):
        channel_name = self.channel_names[self.selected_channel] if self.selected_channel < len(self.channel_names) else "Channel 1"
        unit = self.channel_properties.get(channel_name, {"Unit": "mil"})["Unit"].lower()
        return unit

    def initialize_data_arrays(self):
        self.raw_data = [np.zeros(4096) for _ in range(self.num_channels)]
        self.low_pass_data = [np.zeros(4096) for _ in range(self.num_channels)]
        self.high_pass_data = [np.zeros(4096) for _ in range(self.num_channels)]
        self.band_pass_data = [np.zeros(4096) for _ in range(self.num_channels)]
        self.band_pass_peak_to_peak_history = [[] for _ in range(self.num_channels)]
        self.band_pass_peak_to_peak_times = [[] for _ in range(self.num_channels)]
        self.average_frequency = [0.0 for _ in range(self.num_channels)]
        self.band_pass_peak_to_peak = [0.0 for _ in range(self.num_channels)]
        self.one_x_amps = [[] for _ in range(self.num_channels)]
        self.one_x_phases = [[] for _ in range(self.num_channels)]
        self.two_x_amps = [[] for _ in range(self.num_channels)]
        self.two_x_phases = [[] for _ in range(self.num_channels)]
        self.three_x_amps = [[] for _ in range(self.num_channels)]
        self.three_x_phases = [[] for _ in range(self.num_channels)]
        self.time_points = np.arange(4096) / self.sample_rate
        if self.console:
            self.console.append_to_console(f"Initialized data arrays for {self.num_channels} channels: {self.channel_names}")

    def update_table_defaults(self):
        if not self.table or not self.table_initialized:
            self.log_and_set_status("Table not initialized, skipping update_table_defaults")
            return
        if sip.isdeleted(self.table):
            self.log_and_set_status("Table widget deleted, skipping update_table_defaults")
            return
        headers = ["Channel Name", "Unit", "DateTime", "RPM", "Gap", "Direct", "Bandpass", "1xA", "1xP", "2xA", "2xP", "NXAmp", "NXPhase"]
        self.table.setRowCount(self.num_channels)
        for row in range(self.num_channels):
            channel_name = self.channel_names[row] if row < len(self.channel_names) else f"Channel {row+1}"
            unit = self.channel_properties.get(channel_name, {"Unit": "mil"})["Unit"].lower()
            logging.debug(f"Setting unit for channel {channel_name}: {unit}")
            default_data = {
                "Channel Name": channel_name,
                "Unit": unit,
                "DateTime": datetime.now().strftime("%d-%b-%Y %I:%M:%S %p"),
                "RPM": "0.00", "Gap": "0.00", "Direct": "0.00", "Bandpass": "0.00",
                "1xA": "0.00", "1xP": "0.00", "2xA": "0.00", "2xP": "0.00",
                "NXAmp": "0.00", "NXPhase": "0.00"
            }
            for col, header in enumerate(headers):
                item = QTableWidgetItem(default_data[header])
                item.setTextAlignment(Qt.AlignCenter)
                # Make table values bold
                bold_font = QFont("Times New Roman", 10)
                bold_font.setBold(True)
                item.setFont(bold_font)
                self.table.setItem(row, col, item)
        # Ensure rows use the new padding and height
        self.table.resizeRowsToContents()
        self.adjust_table_height()
        self.update_column_visibility()
        if self.console:
            self.console.append_to_console(f"Updated table defaults with units for {self.num_channels} channels: {self.channel_names}")

    def adjust_table_height(self):
        """Auto-adjust the table's fixed height to fit all rows without an internal scrollbar."""
        if not self.table:
            return
        if sip.isdeleted(self.table):
            return
        header_height = self.table.horizontalHeader().height() if self.table.horizontalHeader() else 0
        # Sum heights of all rows
        total_rows_height = 0
        for r in range(self.table.rowCount()):
            total_rows_height += self.table.rowHeight(r)
        # Add frame width and a small margin
        frame = self.table.frameWidth() * 2
        margin = 6
        new_height = header_height + total_rows_height + frame + margin
        # Put a sensible minimum in case there are no rows yet
        if new_height < header_height + 32 + frame + margin:
            new_height = header_height + 32 + frame + margin
        self.table.setFixedHeight(new_height)

    def load_settings_from_database(self):
        try:
            database = self.mongo_client.get_database("changed_db")
            settings_collection = database.get_collection("TabularViewSettings")
            setting = settings_collection.find_one({"projectId": self.project_id}, sort=[("updated_at", -1)])
            if setting:
                # Custom NX header labels
                self.custom_nx_amp_header = setting.get("customNXAmpHeader", self.custom_nx_amp_header)
                self.custom_nx_phase_header = setting.get("customNXPhaseHeader", self.custom_nx_phase_header)
                # Map DB fields to UI header keys
                self.column_visibility = {
                    "Channel Name": setting.get("channelNameVisible", True),
                    "Unit": setting.get("unitVisible", True),
                    "DateTime": setting.get("datetimeVisible", True),
                    "RPM": setting.get("rpmVisible", True),
                    "Gap": setting.get("gapVisible", True),
                    "Direct": setting.get("directVisible", True),
                    "Bandpass": setting.get("bandpassVisible", True),
                    "1xA": setting.get("oneXaVisible", True),
                    "1xP": setting.get("oneXpVisible", True),
                    "2xA": setting.get("twoXaVisible", True),
                    "2xP": setting.get("twoXpVisible", True),
                    "NXAmp": setting.get("nxAmpVisible", True),
                    "NXPhase": setting.get("nxPhaseVisible", True)
                }
                # Refresh checkbox labels to reflect custom headers
                self.apply_custom_headers()
                for header, cb in self.checkbox_dict.items():
                    cb.setChecked(self.column_visibility.get(header, True))
                # Update inputs if present
                try:
                    if hasattr(self, 'nx_amp_header_input') and self.nx_amp_header_input:
                        self.nx_amp_header_input.setCurrentText(self.custom_nx_amp_header)
                    if hasattr(self, 'nx_phase_header_input') and self.nx_phase_header_input:
                        self.nx_phase_header_input.setCurrentText(self.custom_nx_phase_header)
                except Exception:
                    pass
            self.update_column_visibility()
        except Exception as ex:
            self.log_and_set_status(f"Error loading settings: {str(ex)}")

    def save_settings(self):
        try:
            for header, cb in self.checkbox_dict.items():
                self.column_visibility[header] = cb.isChecked()
            # Save any custom NX header labels from inputs
            try:
                if hasattr(self, 'nx_amp_header_input') and self.nx_amp_header_input:
                    self.custom_nx_amp_header = (self.nx_amp_header_input.currentText() or "NXAmp").strip()
                if hasattr(self, 'nx_phase_header_input') and self.nx_phase_header_input:
                    self.custom_nx_phase_header = (self.nx_phase_header_input.currentText() or "NXPhase").strip()
            except Exception:
                pass
            settings = TabularViewSettings(self.project_id)
            settings.channel_name_visible = self.column_visibility["Channel Name"]
            settings.unit_visible = self.column_visibility["Unit"]
            settings.datetime_visible = self.column_visibility["DateTime"]
            settings.rpm_visible = self.column_visibility["RPM"]
            settings.gap_visible = self.column_visibility["Gap"]
            settings.direct_visible = self.column_visibility["Direct"]
            settings.bandpass_visible = self.column_visibility["Bandpass"]
            # Map UI keys back to DB fields
            settings.one_xa_visible = self.column_visibility["1xA"]
            settings.one_xp_visible = self.column_visibility["1xP"]
            settings.two_xa_visible = self.column_visibility["2xA"]
            settings.two_xp_visible = self.column_visibility["2xP"]
            settings.nx_amp_visible = self.column_visibility["NXAmp"]
            settings.nx_phase_visible = self.column_visibility["NXPhase"]
            database = self.mongo_client.get_database("changed_db")
            settings_collection = database.get_collection("TabularViewSettings")
            settings_collection.insert_one({
                "projectId": self.project_id,
                "channelNameVisible": settings.channel_name_visible,
                "unitVisible": settings.unit_visible,
                "datetimeVisible": settings.datetime_visible,
                "rpmVisible": settings.rpm_visible,
                "gapVisible": settings.gap_visible,
                "directVisible": settings.direct_visible,
                "bandpassVisible": settings.bandpass_visible,
                "oneXaVisible": settings.one_xa_visible,
                "oneXpVisible": settings.one_xp_visible,
                "twoXaVisible": settings.two_xa_visible,
                "twoXpVisible": settings.two_xp_visible,
                "nxAmpVisible": settings.nx_amp_visible,
                "nxPhaseVisible": settings.nx_phase_visible,
                "customNXAmpHeader": self.custom_nx_amp_header,
                "customNXPhaseHeader": self.custom_nx_phase_header,
                "updated_at": settings.updated_at
            })
            self.update_column_visibility()
            # Apply updated header labels to table and checkbox text
            self.apply_custom_headers()
            if self.console:
                self.console.append_to_console("Settings saved successfully")
        except Exception as ex:
            self.log_and_set_status(f"Error saving settings: {str(ex)}")

    def toggle_settings(self):
        opening = not self.settings_panel.isVisible()
        if opening:
            self.settings_panel.setVisible(True)
            self._settings_anim.stop()
            self._settings_anim.setStartValue(self.settings_panel.maximumWidth())
            self._settings_anim.setEndValue(self._settings_width)
            self._settings_anim.start()
            self.settings_button.setVisible(False)
        else:
            self.close_settings()

    def close_settings(self):
        # Animate close then hide
        self._settings_anim.stop()
        self._settings_anim.setStartValue(self.settings_panel.maximumWidth())
        self._settings_anim.setEndValue(0)
        def _after():
            self.settings_panel.setVisible(False)
            self.settings_button.setVisible(True)
        self._settings_anim.finished.connect(_after)
        self._settings_anim.start()
        # Disconnect the temporary slot after it's called once to avoid accumulation
        def _cleanup():
            try:
                self._settings_anim.finished.disconnect(_after)
            except Exception:
                pass
        self._settings_anim.finished.connect(_cleanup)

    def on_column_toggle(self, header, checked):
        # header here is the internal key (we key checkbox_dict by internal) so use as-is
        self.column_visibility[header] = checked
        self.update_column_visibility()

    def update_column_visibility(self):
        # Use internal header order to control visibility
        for col, internal in enumerate(self.internal_headers):
            hidden = not self.column_visibility.get(internal, True)
            self.table.setColumnHidden(col, hidden)

    def get_trigger_indices(self, trigger_data):
        trigger_data = np.array(trigger_data)
        threshold = 0.5
        max_attempts = 5
        for attempt in range(max_attempts):
            indices = []
            for i in range(1, len(trigger_data)):
                if trigger_data[i-1] < threshold and trigger_data[i] >= threshold:
                    indices.append(i)
            if len(indices) >= 2:
                return indices
            threshold /= 2
        # Fallback to artificial triggers
        return [0, 1024, 2048, 3072]

    def compute_harmonics(self, data, start_idx, segment_length, order):
        try:
            if segment_length <= 0 or start_idx >= len(data) or start_idx + segment_length > len(data):
                return 0.0, 0.0
            segment = data[start_idx:start_idx + segment_length]
            N = len(segment)
            if N < 2:
                return 0.0, 0.0
            sine_sum = 0.0
            cosine_sum = 0.0
            for t in range(N):
                angle = 2 * np.pi * order * t / N
                sine_sum += segment[t] * np.sin(angle)
                cosine_sum += segment[t] * np.cos(angle)
            amp = np.sqrt((sine_sum / N)**2 + (cosine_sum / N)**2) * 4
            phase = np.arctan2(cosine_sum, sine_sum) * 180 / np.pi
            return amp, phase
        except Exception as ex:
            self.log_and_set_status(f"Error computing harmonics: {str(ex)}")
            return 0.0, 0.0

    def process_calibrated_data(self, values, ch):
        channel_name = self.channel_names[ch] if ch < len(self.channel_names) else f"Channel {ch+1}"
        props = self.channel_properties.get(channel_name, {
            "CorrectionValue": 1.0,
            "Gain": 1.0,
            "Sensitivity": 1.0,
            "Unit": "mil"
        })
        try:
            volts = (np.array(values, dtype=float) - self.off_set) * self.scaling_factor
            unit = (props.get("Unit", "mil") or "mil").lower()
            if unit == "v":
                calibrated = (volts * props["CorrectionValue"] * props["Gain"]) / props["Sensitivity"]
            elif unit == "mm":
                calibrated = (volts * (props["CorrectionValue"] * props["Gain"])) / props["Sensitivity"]
            elif unit == "um":
                calibrated = (volts * (props["CorrectionValue"] * props["Gain"])) / props["Sensitivity"]
            elif unit == "mil":
                calibrated = (volts * (props["CorrectionValue"] * props["Gain"])) / props["Sensitivity"]
            else:
                calibrated = volts 
            logging.debug(f"Processed data for {channel_name} with unit {unit}, shape: {calibrated.shape}")
            return calibrated
        except Exception as ex:
            self.log_and_set_status(f"Error processing calibrated data for {channel_name}: {str(ex)}")
            return np.zeros(4096)

    def format_direct_value(self, values, unit):
        if not values or len(values) == 0:
            return "0.00"
        # Values are already calibrated to the selected unit; only format
        avg = float(np.mean(values))
        unit = (unit or "mil").lower()
        # Unit-based decimals for amplitude/metrics:
        # mil: 1 decimal, mm: 3 decimals, um: 0 decimals, v: 3 decimals, default: 2
        if unit == "mil":
            return f"{avg:.1f}"
        elif unit == "mm":
            return f"{avg:.3f}"
        elif unit == "um":
            return f"{avg:.0f}"
        elif unit == "v":
            return f"{avg:.3f}"
        else:
            return f"{avg:.2f}"

    def format_direct_bandpass_value(self, value, unit):
        """Format Direct and Bandpass values with unit-specific decimals.
        Rules:
        - mil: 1 decimal
        - mm: 3 decimals
        - um: 0 decimals
        - v: 3 decimals
        """
        try:
            if value is None:
                return "0.0"
            unit = (unit or "mil").lower()
            val = float(value)
            if unit == "mil":
                return f"{val:.1f}"
            elif unit == "mm":
                return f"{val:.3f}"
            elif unit == "um":
                return f"{val:.0f}"
            elif unit == "v":
                return f"{val:.3f}"
            # default fallback
            return f"{val:.2f}"
        except Exception:
            return "0.0"

    def on_data_received(self, tag_name, model_name, values, sample_rate, frame_index):
        if not values or len(values) < 1:
            self.log_and_set_status(f"Insufficient data received for frame {frame_index}: {len(values)} channels")
            return
        self.data_buffer.append((values, sample_rate, frame_index))
        current_time = datetime.now()
        if (current_time - self.last_update_time).total_seconds() >= self.update_interval:
            self.process_buffered_data()
            self.last_update_time = current_time

    def process_buffered_data(self):
        if not self.data_buffer:
            return
        try:
            # Ensure we have the latest channel mapping and units from DB
            self.refresh_channel_properties()
            values, sample_rate, frame_index = self.data_buffer[-1]  # Process the latest data
            self.data_buffer = []  # Clear buffer after processing

            # Dynamically handle channel count and tacho detection
            if len(values) == 0:
                self.log_and_set_status(f"Empty values for frame {frame_index}")
                return
            if not isinstance(values[0], (list, np.ndarray)):
                # Per-channel mode - not expected for TabularView
                self.log_and_set_status(f"Received per-channel data, expected full channels, skipping frame {frame_index}")
                return

            total_channels = len(values)
            expected_main = max(1, len(self.channel_names))  # from model
            inferred_tacho = max(0, total_channels - expected_main)
            if inferred_tacho > 2:
                inferred_tacho = 2
            main_channels = total_channels - inferred_tacho

            # If payload has fewer main channels than expected, shrink arrays but preserve names/units
            if main_channels < expected_main:
                self.log_and_set_status(f"Adjusting channel count from {self.num_channels} to {main_channels} based on payload, frame {frame_index}")
                self.channel_names = self.channel_names[:main_channels] if self.channel_names else [f"Channel_{i+1}" for i in range(main_channels)]
                self.channel_properties = {name: self.channel_properties.get(name, {"Unit": "mil", "CorrectionValue": 1.0, "Gain": 1.0, "Sensitivity": 1.0}) for name in self.channel_names}
                self.num_channels = main_channels
                self.table.setRowCount(self.num_channels)
                self.initialize_data_arrays()
                self.update_table_defaults()
                self.initialize_plots()
            else:
                # Keep model-defined main channel count
                self.num_channels = expected_main

            # Normalize channel lengths to 4096
            for i in range(len(values)):
                if len(values[i]) < 4096:
                    values[i] = list(np.pad(values[i], (0, 4096 - len(values[i])), 'constant'))[:4096]
                elif len(values[i]) > 4096:
                    values[i] = values[i][:4096]

            self.sample_rate = sample_rate if sample_rate and sample_rate > 0 else self.sample_rate
            self.data = values
            if self.console:
                self.console.append_to_console(f"Processing buffered data for frame {frame_index}, mains={self.num_channels}, tacho={inferred_tacho}")

            # Compute triggers from tacho trigger channel (prefer second tacho if present)
            trigger_index = self.num_channels + 1 if inferred_tacho >= 2 else (self.num_channels if inferred_tacho >= 1 else None)
            trigger_data = values[trigger_index] if trigger_index is not None and len(values) > trigger_index else []
            triggers = self.get_trigger_indices(trigger_data) if len(trigger_data) > 0 else [0, 1024, 2048, 3072]

            # Compute Tacho frequency (Hz) from trigger indices
            tacho_freq = 0.0
            if len(triggers) >= 2:
                diffs = np.diff(triggers)
                if len(diffs) > 0:
                    avg_period = float(np.mean(diffs))
                    if avg_period > 0:
                        tacho_freq = float(self.sample_rate) / avg_period

            # Process each main channel
            for ch in range(self.num_channels):
                self.average_frequency[ch] = tacho_freq
                channel_name = self.channel_names[ch] if ch < len(self.channel_names) else f"Channel {ch+1}"
                props = self.channel_properties.get(channel_name, {"Unit": "mil"})
                unit = props["Unit"].lower()
                self.raw_data[ch] = self.process_calibrated_data(values[ch], ch)
                nyquist = self.sample_rate / 2.0
                tap_num = 31
                # Fixed bandpass as per spec (removed selection UI)
                band = [50 / nyquist, 200 / nyquist]
                low_pass_coeffs = signal.firwin(tap_num, 20 / nyquist, window='hamming')
                high_pass_coeffs = signal.firwin(tap_num, 200 / nyquist, window='hamming', pass_zero=False)
                band_pass_coeffs = signal.firwin(tap_num, band, window='hamming', pass_zero=False)
                self.low_pass_data[ch] = signal.lfilter(low_pass_coeffs, 1.0, self.raw_data[ch])
                self.high_pass_data[ch] = signal.lfilter(high_pass_coeffs, 1.0, self.raw_data[ch])
                self.band_pass_data[ch] = signal.lfilter(band_pass_coeffs, 1.0, self.raw_data[ch])

                # Segment-based calculations between triggers
                direct_ptps, bandpass_ptps = [], []
                one_x_amps_list, one_x_phases_list = [], []
                two_x_amps_list, two_x_phases_list = [], []
                three_x_amps_list, three_x_phases_list = [], []
                for j in range(len(triggers) - 1):
                    start = triggers[j]
                    end = triggers[j + 1]
                    seg_len = end - start
                    if seg_len <= 1:
                        continue
                    seg_raw = self.raw_data[ch][start:end]
                    direct_ptps.append(np.max(seg_raw) - np.min(seg_raw))
                    seg_band = self.band_pass_data[ch][start:end]
                    bandpass_ptps.append(np.max(seg_band) - np.min(seg_band))
                    amp1, phase1 = self.compute_harmonics(self.raw_data[ch], start, seg_len, 1)
                    one_x_amps_list.append(amp1)
                    one_x_phases_list.append(phase1)
                    amp2, phase2 = self.compute_harmonics(self.raw_data[ch], start, seg_len, 2)
                    two_x_amps_list.append(amp2)
                    two_x_phases_list.append(phase2)
                    amp3, phase3 = self.compute_harmonics(self.raw_data[ch], start, seg_len, 3)
                    three_x_amps_list.append(amp3)
                    three_x_phases_list.append(phase3)

                avg_direct = float(np.mean(direct_ptps)) if direct_ptps else 0.0
                avg_bandpass = float(np.mean(bandpass_ptps)) if bandpass_ptps else 0.0
                avg_1xa = float(np.mean(one_x_amps_list)) if one_x_amps_list else 0.0
                avg_1xp = float(np.mean(one_x_phases_list)) if one_x_phases_list else 0.0
                avg_2xa = float(np.mean(two_x_amps_list)) if two_x_amps_list else 0.0
                avg_2xp = float(np.mean(two_x_phases_list)) if two_x_phases_list else 0.0
                avg_nxa = float(np.mean(three_x_amps_list)) if three_x_amps_list else 0.0
                avg_nxp = float(np.mean(three_x_phases_list)) if three_x_phases_list else 0.0

                self.band_pass_peak_to_peak[ch] = avg_bandpass
                self.band_pass_peak_to_peak_history[ch].append(avg_bandpass)
                self.band_pass_peak_to_peak_times[ch].append((datetime.now() - self.start_time).total_seconds())
                if len(self.band_pass_peak_to_peak_history[ch]) > 50:
                    self.band_pass_peak_to_peak_history[ch] = self.band_pass_peak_to_peak_history[ch][-50:]
                    self.band_pass_peak_to_peak_times[ch] = self.band_pass_peak_to_peak_times[ch][-50:]

                self.one_x_amps[ch].append(avg_1xa)
                self.one_x_phases[ch].append(avg_1xp)
                self.two_x_amps[ch].append(avg_2xa)
                self.two_x_phases[ch].append(avg_2xp)
                self.three_x_amps[ch].append(avg_nxa)
                self.three_x_phases[ch].append(avg_nxp)
                if len(self.one_x_amps[ch]) > 50:
                    self.one_x_amps[ch] = self.one_x_amps[ch][-50:]
                    self.one_x_phases[ch] = self.one_x_phases[ch][-50:]
                    self.two_x_amps[ch] = self.two_x_amps[ch][-50:]
                    self.two_x_phases[ch] = self.two_x_phases[ch][-50:]
                    self.three_x_amps[ch] = self.three_x_amps[ch][-50:]
                    self.three_x_phases[ch] = self.three_x_phases[ch][-50:]

                channel_data = {
                    "Channel Name": channel_name,
                    "Unit": unit,
                    "DateTime": datetime.now().strftime("%d-%b-%Y %I:%M:%S %p"),
                    "RPM": f"{self.average_frequency[ch] * 60.0:.2f}" if self.average_frequency[ch] > 0 else "0.00",
                    "Gap": (f"{float(self.gap_voltages[ch]):.2f}" if isinstance(self.gap_voltages, (list, tuple)) and ch < len(self.gap_voltages) and self.gap_voltages[ch] is not None else "0.00"),
                    "Direct": self.format_direct_bandpass_value(avg_direct, unit),
                    "Bandpass": self.format_direct_bandpass_value(avg_bandpass, unit),
                    "1xA": self.format_direct_value([avg_1xa], unit),
                    "1xP": f"{avg_1xp:.0f}",
                    "2xA": self.format_direct_value([avg_2xa], unit),
                    "2xP": f"{avg_2xp:.0f}",
                    "NXAmp": self.format_direct_value([avg_nxa], unit),
                    "NXPhase": f"{avg_nxp:.0f}"
                }
                self.update_table_row(ch, channel_data)
            QTimer.singleShot(0, self.update_plots)
            if self.console:
                self.console.append_to_console(f"Processed buffered data for frame {frame_index}, mains={self.num_channels}, tacho={inferred_tacho}")
        except Exception as ex:
            self.log_and_set_status(f"Error processing buffered data for frame {frame_index}: {str(ex)}")

    def load_selected_frame(self, payload: dict):
        try:
            if not payload:
                self.log_and_set_status("TabularView: Invalid selection payload (empty)")
                return
            num_main = int(payload.get("numberOfChannels", 0))
            num_tacho = int(payload.get("tacoChannelCount", 0))
            total_ch = num_main + num_tacho
            Fs = float(payload.get("samplingRate", 0) or 0)
            N = int(payload.get("samplingSize", 0) or 0)
            data_flat = payload.get("message") or payload.get("channelData") or []
            if not Fs or not N or not total_ch or not data_flat:
                self.log_and_set_status("TabularView: Incomplete selection payload (Fs/N/channels/data missing)")
                return

            # Shape to list-of-lists values
            if isinstance(data_flat, list) and data_flat and isinstance(data_flat[0], (int, float)):
                if len(data_flat) != total_ch * N:
                    self.log_and_set_status(f"TabularView: Data length mismatch. Expected {total_ch*N}, got {len(data_flat)}")
                    return
                values = []
                for ch in range(total_ch):
                    start = ch * N
                    end = start + N
                    values.append(data_flat[start:end])
            else:
                values = data_flat
                if len(values) != total_ch or any(len(v) != N for v in values):
                    self.log_and_set_status("TabularView: Invalid nested data shape in selection payload")
                    return

            # Refresh model properties and resize arrays
            self.refresh_channel_properties()
            expected_main = max(1, len(self.channel_names))
            if num_main < expected_main:
                self.channel_names = self.channel_names[:num_main]
            self.num_channels = num_main
            if self.table:
                self.table.setRowCount(self.num_channels)
            self.initialize_data_arrays()
            self.update_table_defaults()

            # Normalize channel lengths to 4096 for internal arrays
            for i in range(len(values)):
                if len(values[i]) < 4096:
                    values[i] = list(np.pad(values[i], (0, 4096 - len(values[i])), 'constant'))[:4096]
                elif len(values[i]) > 4096:
                    values[i] = values[i][:4096]

            self.sample_rate = Fs if Fs > 0 else self.sample_rate

            # Determine trigger channel (prefer second tacho if present)
            inferred_tacho = max(0, total_ch - num_main)
            if inferred_tacho > 2:
                inferred_tacho = 2
            trigger_index = self.num_channels + 1 if inferred_tacho >= 2 else (self.num_channels if inferred_tacho >= 1 else None)
            trigger_data = values[trigger_index] if trigger_index is not None and len(values) > trigger_index else []
            triggers = self.get_trigger_indices(trigger_data) if len(trigger_data) > 0 else [0, 1024, 2048, 3072]

            # Compute per-channel metrics
            for ch in range(self.num_channels):
                self.average_frequency[ch] = 0.0
                if len(triggers) >= 2:
                    diffs = np.diff(triggers)
                    if len(diffs) > 0 and np.mean(diffs) > 0:
                        self.average_frequency[ch] = float(self.sample_rate) / float(np.mean(diffs))

                self.raw_data[ch] = self.process_calibrated_data(values[ch], ch)
                nyquist = self.sample_rate / 2.0
                tap_num = 31
                low_pass_coeffs = signal.firwin(tap_num, 20 / nyquist, window='hamming')
                high_pass_coeffs = signal.firwin(tap_num, 200 / nyquist, window='hamming', pass_zero=False)
                # Fixed bandpass as per spec (removed selection UI)
                band = [50 / nyquist, 200 / nyquist]
                band_pass_coeffs = signal.firwin(tap_num, band, window='hamming', pass_zero=False)

                self.low_pass_data[ch] = signal.lfilter(low_pass_coeffs, 1.0, self.raw_data[ch])
                self.high_pass_data[ch] = signal.lfilter(high_pass_coeffs, 1.0, self.raw_data[ch])
                self.band_pass_data[ch] = signal.lfilter(band_pass_coeffs, 1.0, self.raw_data[ch])

                # Segment computations
                direct_ptps, bandpass_ptps = [], []
                one_x_amps_list, one_x_phases_list = [], []
                two_x_amps_list, two_x_phases_list = [], []
                three_x_amps_list, three_x_phases_list = [], []
                for j in range(len(triggers) - 1):
                    start = triggers[j]
                    end = triggers[j + 1]
                    seg_len = end - start
                    if seg_len <= 1:
                        continue
                    seg_raw = self.raw_data[ch][start:end]
                    direct_ptps.append(float(np.max(seg_raw) - np.min(seg_raw)))
                    seg_band = self.band_pass_data[ch][start:end]
                    bandpass_ptps.append(float(np.max(seg_band) - np.min(seg_band)))
                    amp1, phase1 = self.compute_harmonics(self.raw_data[ch], start, seg_len, 1)
                    one_x_amps_list.append(amp1); one_x_phases_list.append(phase1)
                    amp2, phase2 = self.compute_harmonics(self.raw_data[ch], start, seg_len, 2)
                    two_x_amps_list.append(amp2); two_x_phases_list.append(phase2)
                    amp3, phase3 = self.compute_harmonics(self.raw_data[ch], start, seg_len, 3)
                    three_x_amps_list.append(amp3); three_x_phases_list.append(phase3)

                # Assign single-frame stats
                self.band_pass_peak_to_peak[ch] = float(np.mean(bandpass_ptps)) if bandpass_ptps else 0.0
                self.band_pass_peak_to_peak_history[ch] = [self.band_pass_peak_to_peak[ch]]
                self.band_pass_peak_to_peak_times[ch] = [0.0]
                self.one_x_amps[ch] = [float(np.mean(one_x_amps_list)) if one_x_amps_list else 0.0]
                self.one_x_phases[ch] = [float(np.mean(one_x_phases_list)) if one_x_phases_list else 0.0]
                self.two_x_amps[ch] = [float(np.mean(two_x_amps_list)) if two_x_amps_list else 0.0]
                self.two_x_phases[ch] = [float(np.mean(two_x_phases_list)) if two_x_phases_list else 0.0]
                self.three_x_amps[ch] = [float(np.mean(three_x_amps_list)) if three_x_amps_list else 0.0]
                self.three_x_phases[ch] = [float(np.mean(three_x_phases_list)) if three_x_phases_list else 0.0]

            # Update UI from this single selection
            self.update_display()
            if self.console:
                self.console.append_to_console(
                    f"TabularView: Loaded selected frame {payload.get('frameIndex')} ({N} samples @ {Fs}Hz) for {self.num_channels} channels")
        except Exception as ex:
            self.log_and_set_status(f"TabularView: Error loading selected frame: {str(ex)}")

    def update_table_row(self, row, channel_data):
        if not self.table or not self.table_initialized:
            self.log_and_set_status("Table not initialized, skipping update_table_row")
            return
        if sip.isdeleted(self.table):
            self.log_and_set_status("Table widget deleted, skipping update_table_row")
            return
        headers = ["Channel Name", "Unit", "DateTime", "RPM", "Gap", "Direct", "Bandpass", "1xA", "1xP", "2xA", "2xP", "NXAmp", "NXPhase"]
        try:
            for col, header in enumerate(headers):
                item = QTableWidgetItem(channel_data[header])
                item.setTextAlignment(Qt.AlignCenter)
                # Make table values bold
                bold_font = QFont("Times New Roman", 10)
                bold_font.setBold(True)
                item.setFont(bold_font)
                self.table.setItem(row, col, item)
            logging.debug(f"Updated table row {row} with unit: {channel_data['Unit']}")
            # After updating a row, ensure sizing stays correct
            self.table.resizeRowToContents(row)
        except Exception as ex:
            self.log_and_set_status(f"Error updating table row {row}: {str(ex)}")

    def update_display(self):
        if not self.table or not self.table_initialized:
            self.log_and_set_status("Table not initialized, skipping update_display")
            return
        if sip.isdeleted(self.table):
            self.log_and_set_status("Table widget deleted, skipping update_display")
            return
        try:
            self.process_buffered_data()  # Process any buffered data
            for ch in range(self.num_channels):
                channel_name = self.channel_names[ch] if ch < len(self.channel_names) else f"Channel {ch+1}"
                props = self.channel_properties.get(channel_name, {"Unit": "mil"})
                unit = props["Unit"].lower()
                direct_values = [np.ptp(self.raw_data[ch])] if np.any(self.raw_data[ch]) else []
                channel_data = {
                    "Channel Name": channel_name,
                    "Unit": unit,
                    "DateTime": datetime.now().strftime("%d-%b-%Y %I:%M:%S %p"),
                    "RPM": f"{self.average_frequency[ch] * 60.0:.2f}" if self.average_frequency[ch] > 0 else "0.00",
                    "Gap": (f"{float(self.gap_voltages[ch]):.2f}" if isinstance(self.gap_voltages, (list, tuple)) and ch < len(self.gap_voltages) and self.gap_voltages[ch] is not None else "0.00"),
                    "Direct": self.format_direct_bandpass_value(np.mean(direct_values) if direct_values else 0.0, unit),
                    "Bandpass": self.format_direct_bandpass_value(self.band_pass_peak_to_peak[ch], unit),
                    "1xA": self.format_direct_value([np.mean(self.one_x_amps[ch])], unit) if self.one_x_amps[ch] else "0.00",
                    "1xP": f"{np.mean(self.one_x_phases[ch]):.0f}" if self.one_x_phases[ch] else "0.00",
                    "2xA": self.format_direct_value([np.mean(self.two_x_amps[ch])], unit) if self.two_x_amps[ch] else "0.00",
                    "2xP": f"{np.mean(self.two_x_phases[ch]):.0f}" if self.two_x_phases[ch] else "0.00",
                    "NXAmp": self.format_direct_value([np.mean(self.three_x_amps[ch])], unit) if self.three_x_amps[ch] else "0.00",
                    "NXPhase": f"{np.mean(self.three_x_phases[ch]):.0f}" if self.three_x_phases[ch] else "0.00"
                }
                self.update_table_row(ch, channel_data)
            # After bulk updates, adjust rows and table height
            self.table.resizeRowsToContents()
            self.adjust_table_height()
            # Do not update plots (disabled)
            if self.console:
                self.console.append_to_console(f"Updated display for all {self.num_channels} channels")
        except Exception as ex:
            self.log_and_set_status(f"Error in update_display: {str(ex)}")

    def update_plots(self):
        if not self.plots_enabled:
            return
        if not self.plot_initialized or not self.plot_widgets or not self.plots:
            self.initialize_plots()
            return
        ch = self.selected_channel
        if ch >= self.num_channels:
            self.log_and_set_status("Selected channel not available, skipping plot update")
            return
        trim_samples = 47
        low_pass_trim = 0
        high_pass_trim = 110
        band_pass_trim = 110
        raw_trim = trim_samples
        if len(self.raw_data[ch]) <= trim_samples:
            raw_trim = low_pass_trim = high_pass_trim = band_pass_trim = 0
        channel_name = self.channel_names[ch] if ch < len(self.channel_names) else f"Channel {ch+1}"
        unit = self.channel_properties.get(channel_name, {"Unit": "mil"})["Unit"].lower()
        data_sets = [
            (self.raw_data[ch], raw_trim, "Raw Data"),
            (self.low_pass_data[ch], low_pass_trim, "Low-Pass Filtered Data (20 Hz)"),
            (self.high_pass_data[ch], high_pass_trim, "High-Pass Filtered Data (200 Hz)"),
            (self.band_pass_data[ch], band_pass_trim, "Band-Pass Filtered Data (50-200 Hz)")
        ]
        for i, (data, trim, title) in enumerate(data_sets):
            try:
                if len(data) <= trim:
                    data = np.array([0])
                    time_data = np.array([0])
                else:
                    data = data[trim:][:4096]  # Limit to 4096 samples for plotting
                    time_data = self.time_points[:len(data)]
                if i < len(self.plots):
                    self.plots[i].setData(time_data, data)
                    self.plot_widgets[i].setTitle(f"{title} (Channel: {self.channel_names[ch]}, Freq: {self.average_frequency[ch]:.2f} Hz, Unit: {unit})")
                    y_min = np.min(data) * 1.1 if data.size > 0 else -1.0
                    y_max = np.max(data) * 1.1 if data.size > 0 else 1.0
                    self.plot_widgets[i].setYRange(y_min, y_max, padding=0.1)
            except Exception as ex:
                self.log_and_set_status(f"Error updating plot {i}: {str(ex)}")
        try:
            if self.band_pass_peak_to_peak_times[ch] and self.band_pass_peak_to_peak_history[ch]:
                peak_data = np.array(self.band_pass_peak_to_peak_history[ch])
                if unit == "mm":
                    peak_data /= 25.4
                elif unit == "um":
                    peak_data *= 25.4 * 1000
                self.plots[4].setData(self.band_pass_peak_to_peak_times[ch], peak_data)
                y_max = max(0.01, np.max(peak_data) * 1.1) if peak_data.size > 0 else 0.01
                self.plot_widgets[4].setYRange(0, y_max, padding=0.1)
            else:
                self.plots[4].setData(np.array([0]), np.array([0]))
                self.plot_widgets[4].setYRange(0, 0.01, padding=0.1)
            self.plot_widgets[4].setTitle(f"Bandpass Peak-to-Peak Over Time (Channel: {self.channel_names[ch]}, Unit: {unit})")
        except Exception as ex:
            self.log_and_set_status(f"Error updating peak-to-peak plot: {str(ex)}")

    def refresh_channel_properties(self):
        try:
            project_data = self.db.get_project_data(self.project_name)
            if not project_data:
                self.log_and_set_status(f"Project {self.project_name} not found")
                return
            model = next((m for m in project_data["models"] if m["name"] == self.model_name), None)
            if not model:
                self.log_and_set_status(f"Model {self.model_name} not found")
                return
            self.channel_names = [c.get("channelName", f"Channel {i+1}") for i, c in enumerate(model["channels"])]
            self.num_channels = len(self.channel_names)
            self.channel_properties = {}
            for channel in model["channels"]:
                channel_name = channel.get("channelName", "Unknown")
                unit = channel.get("unit", "mil").lower().strip()
                self.channel_properties[channel_name] = {
                    "Unit": unit,
                    "CorrectionValue": float(channel.get("correctionValue", "1.0")) if channel.get("correctionValue") else 1.0,
                    "Gain": float(channel.get("gain", "1.0")) if channel.get("gain") else 1.0,
                    "Sensitivity": float(channel.get("sensitivity", "1.0")) if channel.get("sensitivity") else 1.0
                }
            self.table.setRowCount(self.num_channels)
            self.initialize_data_arrays()
            self.update_table_defaults()
            self.load_settings_from_database()
            if self.console:
                self.console.append_to_console(f"Refreshed channel properties: {self.num_channels} channels, Names: {self.channel_names}, Units: {[self.channel_properties[name]['Unit'] for name in self.channel_names]}")
        except Exception as ex:
            self.log_and_set_status(f"Error refreshing channel properties: {str(ex)}")

    def log_and_set_status(self, message):
        logging.error(message)
        if self.console:
            self.console.append_to_console(message)

    def close(self):
        self.timer.stop()
        if hasattr(self, 'thread') and self.thread.isRunning():
            self.thread.quit()
            self.thread.wait()
        for widget in self.plot_widgets:
            self.scroll_layout.removeWidget(widget)
            widget.deleteLater()
        self.plot_widgets = []
        self.plots = []
        if self.table:
            self.table.deleteLater()
            self.table = None
            self.table_initialized = False
        if self.widget:
            self.widget.deleteLater()
            self.widget = None

    def get_widget(self):
        if not self.widget:
            self.log_and_set_status("Widget not initialized, recreating UI")
            self.initUI()
        return self.widget

    def set_gap_voltages(self, gaps):
        """Update the latest gap voltages read from MQTT header[15..28] (already scaled by 1/100, signed)."""
        try:
            if not isinstance(gaps, (list, tuple)):
                return
            # Store as floats; may be longer than channel count; we index by channel index when using
            self.gap_voltages = [float(x) if x is not None else None for x in gaps]
            # Trigger a non-blocking UI refresh so Gap column updates
            QTimer.singleShot(0, self.update_display)
        except Exception as ex:
            self.log_and_set_status(f"Error setting gap voltages: {str(ex)}")