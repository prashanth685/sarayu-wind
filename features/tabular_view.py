import numpy as np
from PyQt5.QtWidgets import QWidget, QVBoxLayout, QTableWidget, QTableWidgetItem, QScrollArea, QPushButton, QCheckBox, QComboBox, QHBoxLayout, QGridLayout, QLabel, QSizePolicy, QHeaderView, QDoubleSpinBox
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
                    "Subunit": str(channel.get("subunit", "pk-pk")).lower().strip() or "pk-pk",
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
            "2xPhase": True, "NX1Amp": True, "NX1Phase": True, "NX2Amp": True, "NX2Phase": True,
            "NX3Amp": True, "NX3Phase": True
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
            "1xAmp", "1xPhase", "2xAmp", "2xPhase",
            "NX1Amp", "NX1Phase", "NX2Amp", "NX2Phase", "NX3Amp", "NX3Phase"
        ]
        self.custom_nx_amp_header = "NXAmp"
        self.custom_nx_phase_header = "NXPhase"
        # NX harmonic selections
        # Legacy single selection (kept for backward compatibility)
        self.nx_amp_selection = 3
        self.nx_phase_selection = 3
        # New: three independent NX selections and headers
        self.nx1_selection = 1.0
        self.nx2_selection = 2.0
        self.nx3_selection = 3.0
        self.custom_nx1_amp_header = f"{self.nx1_selection}xAmp"
        self.custom_nx1_phase_header = f"{self.nx1_selection}xPhase"
        self.custom_nx2_amp_header = f"{self.nx2_selection}xAmp"
        self.custom_nx2_phase_header = f"{self.nx2_selection}xPhase"
        self.custom_nx3_amp_header = f"{self.nx3_selection}xAmp"
        self.custom_nx3_phase_header = f"{self.nx3_selection}xPhase"
        # Performance: cache filter coefficients and throttle expensive UI ops
        self._last_filter_rate = None
        self._low_pass_coeffs = None
        self._high_pass_coeffs = None
        self._band_pass_coeffs = None
        self._last_props_refresh = datetime.min
        self._props_refresh_interval_sec = 5  # avoid DB lookups more than every 5s
        self._last_table_resize = datetime.min
        self._table_resize_interval_sec = 2  # resize rows/height at most every 2s
        self._last_log_time = datetime.min
        self._log_interval_sec = 5  # reduce console spam
        # Table sizing behavior: auto-fit height unless settings panel is open
        self._auto_table_height = True
        self._prev_table_height = None
        self.initUI()
        self.initialize_thread()

    def get_display_headers(self):
        """Return header labels for display; include selected NX values (e.g., NX1 3x Amp)."""
        sel1 = self._format_nx_value(self.nx1_selection)
        sel2 = self._format_nx_value(self.nx2_selection)
        sel3 = self._format_nx_value(self.nx3_selection)
        mapping = {
            "NX1Amp": f"NX1 {sel1}x Amp",
            "NX1Phase": f"NX1 {sel1}x Phase",
            "NX2Amp": f"NX2 {sel2}x Amp",
            "NX2Phase": f"NX2 {sel2}x Phase",
            "NX3Amp": f"NX3 {sel3}x Amp",
            "NX3Phase": f"NX3 {sel3}x Phase",
        }
        headers = [mapping.get(key, key) for key in self.internal_headers]
        return headers

    def apply_custom_headers(self):
        """Apply current custom NX headers to the table and settings checkboxes UI."""
        try:
            if self.table:
                self.table.setHorizontalHeaderLabels(self.get_display_headers())
            # Update checkbox labels but keep internal keys in the dict
            try:
                if "NX1Amp" in self.checkbox_dict and self.checkbox_dict["NX1Amp"]:
                    self.checkbox_dict["NX1Amp"].setText(f"NX1 {self._format_nx_value(self.nx1_selection)}x Amp")
            except Exception:
                pass
            try:
                if "NX1Phase" in self.checkbox_dict and self.checkbox_dict["NX1Phase"]:
                    self.checkbox_dict["NX1Phase"].setText(f"NX1 {self._format_nx_value(self.nx1_selection)}x Phase")
            except Exception:
                pass
            try:
                if "NX2Amp" in self.checkbox_dict and self.checkbox_dict["NX2Amp"]:
                    self.checkbox_dict["NX2Amp"].setText(f"NX2 {self._format_nx_value(self.nx2_selection)}x Amp")
            except Exception:
                pass
            try:
                if "NX2Phase" in self.checkbox_dict and self.checkbox_dict["NX2Phase"]:
                    self.checkbox_dict["NX2Phase"].setText(f"NX2 {self._format_nx_value(self.nx2_selection)}x Phase")
            except Exception:
                pass
            try:
                if "NX3Amp" in self.checkbox_dict and self.checkbox_dict["NX3Amp"]:
                    self.checkbox_dict["NX3Amp"].setText(f"NX3 {self._format_nx_value(self.nx3_selection)}x Amp")
            except Exception:
                pass
            try:
                if "NX3Phase" in self.checkbox_dict and self.checkbox_dict["NX3Phase"]:
                    self.checkbox_dict["NX3Phase"].setText(f"NX3 {self._format_nx_value(self.nx3_selection)}x Phase")
            except Exception:
                pass
        except Exception:
            pass

    def _format_nx_value(self, v):
        """Format a numeric NX value into a compact string matching dropdown entries."""
        try:
            f = float(v)
            s = f"{f:.2f}".rstrip('0').rstrip('.')
            return s
        except Exception:
            return "3"

    def on_nx_selection_changed(self, _text=None):
        """Keep table headers and checkbox labels in sync with live NX dropdown changes."""
        try:
            if hasattr(self, 'nx1_input') and self.nx1_input:
                self.nx1_selection = float(self.nx1_input.currentText())
            if hasattr(self, 'nx2_input') and self.nx2_input:
                self.nx2_selection = float(self.nx2_input.currentText())
            if hasattr(self, 'nx3_input') and self.nx3_input:
                self.nx3_selection = float(self.nx3_input.currentText())
        except Exception:
            pass
        # Update headers and checkbox labels
        self.apply_custom_headers()
        # Resize headers to fit new text
        try:
            self.table.resizeColumnsToContents()
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

        # NX Harmonic selections: dropdowns for NX1, NX2, NX3
        self._nx_allowed_values = ["0.25","0.47","0.48","0.5","0.75","1","2","3","4","5","6","7","8","9","10"]
        nx1_label = QLabel("NX1 Harmonic:")
        self.nx1_input = QComboBox(); self.nx1_input.addItems(self._nx_allowed_values)
        self.nx1_input.setCurrentText(self._format_nx_value(self.nx1_selection))
        self.nx1_input.currentTextChanged.connect(self.on_nx_selection_changed)
        nx2_label = QLabel("NX2 Harmonic:")
        self.nx2_input = QComboBox(); self.nx2_input.addItems(self._nx_allowed_values)
        self.nx2_input.setCurrentText(self._format_nx_value(self.nx2_selection))
        self.nx2_input.currentTextChanged.connect(self.on_nx_selection_changed)
        nx3_label = QLabel("NX3 Harmonic:")
        self.nx3_input = QComboBox(); self.nx3_input.addItems(self._nx_allowed_values)
        self.nx3_input.setCurrentText(self._format_nx_value(self.nx3_selection))
        self.nx3_input.currentTextChanged.connect(self.on_nx_selection_changed)
        # Place NX rows compactly
        settings_layout.addWidget(nx1_label, 1, 0)
        settings_layout.addWidget(self.nx1_input, 1, 1)
        settings_layout.addWidget(nx2_label, 2, 0)
        settings_layout.addWidget(self.nx2_input, 2, 1)
        settings_layout.addWidget(nx3_label, 3, 0)
        settings_layout.addWidget(self.nx3_input, 3, 1)

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
            if header == "NX1Amp":
                display_text = self.custom_nx1_amp_header or header
            elif header == "NX1Phase":
                display_text = self.custom_nx1_phase_header or header
            elif header == "NX2Amp":
                display_text = self.custom_nx2_amp_header or header
            elif header == "NX2Phase":
                display_text = self.custom_nx2_phase_header or header
            elif header == "NX3Amp":
                display_text = self.custom_nx3_amp_header or header
            elif header == "NX3Phase":
                display_text = self.custom_nx3_phase_header or header
            cb = QCheckBox(display_text)
            cb.setChecked(self.column_visibility.get(key, True))
            cb.setStyleSheet("font-size: 14px;")
            # Immediate apply on toggle
            cb.toggled.connect(lambda checked, h=key: self.on_column_toggle(h, checked))
            self.checkbox_dict[key] = cb
            opts_layout.addWidget(cb)
        opts_layout.addStretch()
        opts_scroll.setWidget(opts_container)
        # Place the options scroll area below NX rows
        opts_row = 4
        settings_layout.addWidget(opts_scroll, opts_row, 0, 1, 3)

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
        buttons_row = 5
        settings_layout.setRowStretch(opts_row, 1)  # make the scroll area take remaining space
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
        # Default: remove internal vertical scrollbar; allow horizontal scroll for many columns
        self.table.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.table.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        # Header resize behavior: fit contents so columns remain readable
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        # Fill any remaining space to avoid empty gaps
        self.table.horizontalHeader().setStretchLastSection(True)
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
        # NX1/NX2/NX3 arrays
        self.nx1_amps = [[] for _ in range(self.num_channels)]
        self.nx1_phases = [[] for _ in range(self.num_channels)]
        self.nx2_amps = [[] for _ in range(self.num_channels)]
        self.nx2_phases = [[] for _ in range(self.num_channels)]
        self.nx3_amps = [[] for _ in range(self.num_channels)]
        self.nx3_phases = [[] for _ in range(self.num_channels)]
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
        headers = list(self.internal_headers)
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
                "1xAmp": "0.00", "1xPhase": "0.00", "2xAmp": "0.00", "2xPhase": "0.00",
                "NX1Amp": "0.00", "NX1Phase": "0.00", "NX2Amp": "0.00", "NX2Phase": "0.00", "NX3Amp": "0.00", "NX3Phase": "0.00"
            }
            for col, internal in enumerate(headers):
                item = QTableWidgetItem(default_data[internal])
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
        # If auto-height is disabled (e.g., settings panel open), do not change height here
        if not getattr(self, "_auto_table_height", True):
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
                # Load NX selections; fallback to legacy nxAmpSelection if provided
                try:
                    self.nx1_selection = float(setting.get("nx1Selection", self.nx1_selection))
                except Exception:
                    pass
                try:
                    self.nx2_selection = float(setting.get("nx2Selection", self.nx2_selection))
                except Exception:
                    pass
                try:
                    self.nx3_selection = float(setting.get("nx3Selection", setting.get("nxAmpSelection", self.nx3_selection)))
                except Exception:
                    pass
                # Update custom headers for NX1..NX3
                self.custom_nx1_amp_header = f"{self.nx1_selection}xAmp"; self.custom_nx1_phase_header = f"{self.nx1_selection}xPhase"
                self.custom_nx2_amp_header = f"{self.nx2_selection}xAmp"; self.custom_nx2_phase_header = f"{self.nx2_selection}xPhase"
                self.custom_nx3_amp_header = f"{self.nx3_selection}xAmp"; self.custom_nx3_phase_header = f"{self.nx3_selection}xPhase"
                # Map DB fields to UI header keys
                self.column_visibility = {
                    "Channel Name": setting.get("channelNameVisible", True),
                    "Unit": setting.get("unitVisible", True),
                    "DateTime": setting.get("datetimeVisible", True),
                    "RPM": setting.get("rpmVisible", True),
                    "Gap": setting.get("gapVisible", True),
                    "Direct": setting.get("directVisible", True),
                    "Bandpass": setting.get("bandpassVisible", True),
                    "1xAmp": setting.get("oneXaVisible", True),
                    "1xPhase": setting.get("oneXpVisible", True),
                    "2xAmp": setting.get("twoXaVisible", True),
                    "2xPhase": setting.get("twoXpVisible", True),
                    "NX1Amp": setting.get("nx1AmpVisible", True),
                    "NX1Phase": setting.get("nx1PhaseVisible", True),
                    "NX2Amp": setting.get("nx2AmpVisible", True),
                    "NX2Phase": setting.get("nx2PhaseVisible", True),
                    "NX3Amp": setting.get("nx3AmpVisible", True),
                    "NX3Phase": setting.get("nx3PhaseVisible", True),
                }
                # Refresh checkbox labels and states
                self.apply_custom_headers()
                for header, cb in self.checkbox_dict.items():
                    cb.setChecked(self.column_visibility.get(header, True))
                # Update inputs if present
                try:
                    if hasattr(self, 'nx1_input') and self.nx1_input:
                        self.nx1_input.setCurrentText(self._format_nx_value(self.nx1_selection))
                    if hasattr(self, 'nx2_input') and self.nx2_input:
                        self.nx2_input.setCurrentText(self._format_nx_value(self.nx2_selection))
                    if hasattr(self, 'nx3_input') and self.nx3_input:
                        self.nx3_input.setCurrentText(self._format_nx_value(self.nx3_selection))
                except Exception:
                    pass
            self.update_column_visibility()
        except Exception as ex:
            self.log_and_set_status(f"Error loading settings: {str(ex)}")

    def save_settings(self):
        try:
            for header, cb in self.checkbox_dict.items():
                self.column_visibility[header] = cb.isChecked()
            # Read harmonic selections from dropdowns
            try:
                if hasattr(self, 'nx1_input') and self.nx1_input:
                    self.nx1_selection = float(self.nx1_input.currentText())
                if hasattr(self, 'nx2_input') and self.nx2_input:
                    self.nx2_selection = float(self.nx2_input.currentText())
                if hasattr(self, 'nx3_input') and self.nx3_input:
                    self.nx3_selection = float(self.nx3_input.currentText())
            except Exception:
                pass
            # Update headers
            self.custom_nx1_amp_header = f"{self.nx1_selection}xAmp"; self.custom_nx1_phase_header = f"{self.nx1_selection}xPhase"
            self.custom_nx2_amp_header = f"{self.nx2_selection}xAmp"; self.custom_nx2_phase_header = f"{self.nx2_selection}xPhase"
            self.custom_nx3_amp_header = f"{self.nx3_selection}xAmp"; self.custom_nx3_phase_header = f"{self.nx3_selection}xPhase"
            settings = TabularViewSettings(self.project_id)
            settings.channel_name_visible = self.column_visibility["Channel Name"]
            settings.unit_visible = self.column_visibility["Unit"]
            settings.datetime_visible = self.column_visibility["DateTime"]
            settings.rpm_visible = self.column_visibility["RPM"]
            settings.gap_visible = self.column_visibility["Gap"]
            settings.direct_visible = self.column_visibility["Direct"]
            settings.bandpass_visible = self.column_visibility["Bandpass"]
            # Map UI keys back to DB fields
            settings.one_xa_visible = self.column_visibility["1xAmp"]
            settings.one_xp_visible = self.column_visibility["1xPhase"]
            settings.two_xa_visible = self.column_visibility["2xAmp"]
            settings.two_xp_visible = self.column_visibility["2xPhase"]
            # Additional NX visibilities are stored directly in the document
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
                # NX1/NX2/NX3 visibility
                "nx1AmpVisible": self.column_visibility.get("NX1Amp", True),
                "nx1PhaseVisible": self.column_visibility.get("NX1Phase", True),
                "nx2AmpVisible": self.column_visibility.get("NX2Amp", True),
                "nx2PhaseVisible": self.column_visibility.get("NX2Phase", True),
                "nx3AmpVisible": self.column_visibility.get("NX3Amp", True),
                "nx3PhaseVisible": self.column_visibility.get("NX3Phase", True),
                # Selections
                "nx1Selection": float(self.nx1_selection),
                "nx2Selection": float(self.nx2_selection),
                "nx3Selection": float(self.nx3_selection),
                # Legacy compatibility fields (map NX3)
                "nxAmpSelection": float(self.nx3_selection),
                "nxPhaseSelection": float(self.nx3_selection),
                # Custom headers
                "customNX1AmpHeader": self.custom_nx1_amp_header,
                "customNX1PhaseHeader": self.custom_nx1_phase_header,
                "customNX2AmpHeader": self.custom_nx2_amp_header,
                "customNX2PhaseHeader": self.custom_nx2_phase_header,
                "customNX3AmpHeader": self.custom_nx3_amp_header,
                "customNX3PhaseHeader": self.custom_nx3_phase_header,
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
            # When settings open: enable table vertical scrolling and set a compact fixed height
            try:
                if self._auto_table_height:
                    self._prev_table_height = self.table.height()
                self._auto_table_height = False
                self.table.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
                # Set a reasonable compact height to allow scrolling
                self.table.setFixedHeight(max(420, min(840, self.table.height())))
            except Exception:
                pass
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
            # Restore table auto-height and remove internal vertical scrollbar
            try:
                self._auto_table_height = True
                self.table.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
                # Restore previous height if remembered; otherwise auto-adjust
                if self._prev_table_height is not None:
                    # Temporarily set then auto-adjust to actual content
                    self.table.setFixedHeight(self._prev_table_height)
                    self._prev_table_height = None
                # Apply auto adjust to fit all rows
                self.adjust_table_height()
            except Exception:
                pass
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
        try:
            self.table.resizeColumnsToContents()
        except Exception:
            pass

    def update_column_visibility(self):
        # Use internal header order to control visibility
        for col, internal in enumerate(self.internal_headers):
            hidden = not self.column_visibility.get(internal, True)
            self.table.setColumnHidden(col, hidden)
        # After visibility changes, adjust columns to avoid leftover space
        try:
            self.table.resizeColumnsToContents()
        except Exception:
            pass

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

    def _convert_ptp_by_subunit(self, ptp_value, subunit):
        """Convert a peak-to-peak value into the desired subunit.
        subunit: 'pk-pk' => unchanged, 'pk' => ptp/2, 'rms' => ptp/(2*sqrt(2)) assuming sinusoid.
        """
        try:
            if ptp_value is None:
                return 0.0
            sub = (subunit or "pk-pk").lower().strip()
            v = float(ptp_value)
            if sub == "pk":
                return v / 2.0
            if sub == "rms":
                return v / (2.0 * np.sqrt(2.0))
            return v
        except Exception:
            return float(ptp_value or 0.0)

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
            # Rate-limit DB-backed refreshes to avoid UI stalls
            now = datetime.now()
            if (now - self._last_props_refresh).total_seconds() >= self._props_refresh_interval_sec:
                self.refresh_channel_properties()
                self._last_props_refresh = now
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
            if self.console and (datetime.now() - self._last_log_time).total_seconds() >= self._log_interval_sec:
                self.console.append_to_console(f"Processing buffered data for frame {frame_index}, mains={self.num_channels}, tacho={inferred_tacho}")
                self._last_log_time = datetime.now()

            # Compute triggers from tacho trigger channel (prefer second tacho if present)
            trigger_index = self.num_channels + 1 if inferred_tacho >= 2 else (self.num_channels if inferred_tacho >= 1 else None)
            trigger_data = values[trigger_index] if trigger_index is not None and len(values) > trigger_index else []
            triggers = self.get_trigger_indices(trigger_data) if len(trigger_data) > 0 else [0, 1024, 2048, 3072]

            # Compute Tacho frequency (Hz) from trigger indices
            tacho_freq = 0.0
            # Prefer direct frequency channel if present (first tacho channel after main channels)
            try:
                freq_ch_idx = self.num_channels if inferred_tacho >= 1 else None
                if freq_ch_idx is not None and len(values) > freq_ch_idx and len(values[freq_ch_idx]) > 0:
                    # Use mean of positive frequencies to avoid zeros/spikes
                    freq_vals = np.array(values[freq_ch_idx], dtype=float)
                    freq_vals = freq_vals[np.isfinite(freq_vals) & (freq_vals > 0)]
                    if freq_vals.size > 0:
                        # Frequency arrives scaled x100; convert back to Hz
                        tacho_freq = float(np.mean(freq_vals)) / 100.0
            except Exception:
                # Ignore and fallback to trigger-based estimation
                pass
            # Fallback to trigger-based estimation if no valid freq found
            if tacho_freq <= 0.0 and len(triggers) >= 2:
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
                # Ensure cached FIR coefficients for current rate
                self._ensure_filters()
                self.low_pass_data[ch] = signal.lfilter(self._low_pass_coeffs, 1.0, self.raw_data[ch])
                self.high_pass_data[ch] = signal.lfilter(self._high_pass_coeffs, 1.0, self.raw_data[ch])
                self.band_pass_data[ch] = signal.lfilter(self._band_pass_coeffs, 1.0, self.raw_data[ch])

                # Segment-based calculations between triggers
                direct_ptps, bandpass_ptps = [], []
                one_x_amps_list, one_x_phases_list = [], []
                two_x_amps_list, two_x_phases_list = [], []
                nx1_amp_list, nx1_phase_list = [], []
                nx2_amp_list, nx2_phase_list = [], []
                nx3_amp_list, nx3_phase_list = [], []
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
                    # NX1/NX2/NX3 selections
                    for n_val, a_list, p_list in [
                        (self.nx1_selection, nx1_amp_list, nx1_phase_list),
                        (self.nx2_selection, nx2_amp_list, nx2_phase_list),
                        (self.nx3_selection, nx3_amp_list, nx3_phase_list),
                    ]:
                        try:
                            n = float(n_val)
                        except Exception:
                            n = 3.0
                        aN, _ = self.compute_harmonics(self.raw_data[ch], start, seg_len, n)
                        _, pN = self.compute_harmonics(self.raw_data[ch], start, seg_len, n)
                        a_list.append(aN); p_list.append(pN)

                avg_direct = float(np.mean(direct_ptps)) if direct_ptps else 0.0
                avg_bandpass = float(np.mean(bandpass_ptps)) if bandpass_ptps else 0.0
                avg_1xa = float(np.mean(one_x_amps_list)) if one_x_amps_list else 0.0
                avg_1xp = float(np.mean(one_x_phases_list)) if one_x_phases_list else 0.0
                avg_2xa = float(np.mean(two_x_amps_list)) if two_x_amps_list else 0.0
                avg_2xp = float(np.mean(two_x_phases_list)) if two_x_phases_list else 0.0
                avg_nx1a = float(np.mean(nx1_amp_list)) if nx1_amp_list else 0.0
                avg_nx1p = float(np.mean(nx1_phase_list)) if nx1_phase_list else 0.0
                avg_nx2a = float(np.mean(nx2_amp_list)) if nx2_amp_list else 0.0
                avg_nx2p = float(np.mean(nx2_phase_list)) if nx2_phase_list else 0.0
                avg_nx3a = float(np.mean(nx3_amp_list)) if nx3_amp_list else 0.0
                avg_nx3p = float(np.mean(nx3_phase_list)) if nx3_phase_list else 0.0

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
                self.nx1_amps[ch].append(avg_nx1a); self.nx1_phases[ch].append(avg_nx1p)
                self.nx2_amps[ch].append(avg_nx2a); self.nx2_phases[ch].append(avg_nx2p)
                self.nx3_amps[ch].append(avg_nx3a); self.nx3_phases[ch].append(avg_nx3p)
                if len(self.one_x_amps[ch]) > 50:
                    self.one_x_amps[ch] = self.one_x_amps[ch][-50:]
                    self.one_x_phases[ch] = self.one_x_phases[ch][-50:]
                    self.two_x_amps[ch] = self.two_x_amps[ch][-50:]
                    self.two_x_phases[ch] = self.two_x_phases[ch][-50:]
                    self.nx1_amps[ch] = self.nx1_amps[ch][-50:]
                    self.nx1_phases[ch] = self.nx1_phases[ch][-50:]
                    self.nx2_amps[ch] = self.nx2_amps[ch][-50:]
                    self.nx2_phases[ch] = self.nx2_phases[ch][-50:]
                    self.nx3_amps[ch] = self.nx3_amps[ch][-50:]
                    self.nx3_phases[ch] = self.nx3_phases[ch][-50:]

                channel_data = {
                    "Channel Name": channel_name,
                    "Unit": unit,
                    "DateTime": datetime.now().strftime("%d-%b-%Y %I:%M:%S %p"),
                    "RPM": f"{int(round(self.average_frequency[ch] * 60.0))}" if self.average_frequency[ch] > 0 else "0",
                    "Gap": (f"{float(self.gap_voltages[ch]):.2f}" if isinstance(self.gap_voltages, (list, tuple)) and ch < len(self.gap_voltages) and self.gap_voltages[ch] is not None else "0.00"),
                    "Direct": self.format_direct_bandpass_value(avg_direct, unit),
                    "Bandpass": self.format_direct_bandpass_value(avg_bandpass, unit),
                    "1xAmp": self.format_direct_value([avg_1xa], unit),
                    "1xPhase": f"{avg_1xp:.0f}°",
                    "2xAmp": self.format_direct_value([avg_2xa], unit),
                    "2xPhase": f"{avg_2xp:.0f}°",
                    "NX1Amp": self.format_direct_value([avg_nx1a], unit),
                    "NX1Phase": f"{avg_nx1p:.0f}°",
                    "NX2Amp": self.format_direct_value([avg_nx2a], unit),
                    "NX2Phase": f"{avg_nx2p:.0f}°",
                    "NX3Amp": self.format_direct_value([avg_nx3a], unit),
                    "NX3Phase": f"{avg_nx3p:.0f}°"
                }
                self.update_table_row(ch, channel_data)
            QTimer.singleShot(0, self.update_plots)
            if self.console and (datetime.now() - self._last_log_time).total_seconds() >= self._log_interval_sec:
                self.console.append_to_console(f"Processed buffered data for frame {frame_index}, mains={self.num_channels}, tacho={inferred_tacho}")
                self._last_log_time = datetime.now()
        except Exception as ex:
            self.log_and_set_status(f"Error processing buffered data for frame {frame_index}: {str(ex)}")

    def _ensure_filters(self):
        """Compute and cache FIR coefficients for the current sample rate."""
        try:
            if self._last_filter_rate == self.sample_rate and self._low_pass_coeffs is not None:
                return
            nyquist = max(1.0, float(self.sample_rate) / 2.0)
            tap_num = 31
            band = [50 / nyquist, 200 / nyquist]
            # Cache coefficients
            self._low_pass_coeffs = signal.firwin(tap_num, 20 / nyquist, window='hamming')
            self._high_pass_coeffs = signal.firwin(tap_num, 200 / nyquist, window='hamming', pass_zero=False)
            self._band_pass_coeffs = signal.firwin(tap_num, band, window='hamming', pass_zero=False)
            self._last_filter_rate = self.sample_rate
        except Exception as ex:
            self.log_and_set_status(f"Error computing filter coefficients: {str(ex)}")

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
            # Prefer direct frequency channel if present; fallback to trigger-based estimation
            freq_from_channel = 0.0
            try:
                freq_ch_idx = self.num_channels if inferred_tacho >= 1 else None
                if freq_ch_idx is not None and len(values) > freq_ch_idx and len(values[freq_ch_idx]) > 0:
                    fvals = np.array(values[freq_ch_idx], dtype=float)
                    fvals = fvals[np.isfinite(fvals) & (fvals > 0)]
                    if fvals.size > 0:
                        # Frequency arrives scaled x100; convert back to Hz
                        freq_from_channel = float(np.mean(fvals)) / 100.0
            except Exception:
                pass
            trig_based_freq = 0.0
            if len(triggers) >= 2:
                diffs = np.diff(triggers)
                if len(diffs) > 0 and np.mean(diffs) > 0:
                    trig_based_freq = float(self.sample_rate) / float(np.mean(diffs))

            for ch in range(self.num_channels):
                self.average_frequency[ch] = freq_from_channel if freq_from_channel > 0.0 else trig_based_freq

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
                nx1_amp_list, nx1_phase_list = [], []
                nx2_amp_list, nx2_phase_list = [], []
                nx3_amp_list, nx3_phase_list = [], []
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
                    # NX1/NX2/NX3 selections
                    for n_val, a_list, p_list in [
                        (self.nx1_selection, nx1_amp_list, nx1_phase_list),
                        (self.nx2_selection, nx2_amp_list, nx2_phase_list),
                        (self.nx3_selection, nx3_amp_list, nx3_phase_list),
                    ]:
                        try:
                            n = float(n_val)
                        except Exception:
                            n = 3.0
                        aN, _ = self.compute_harmonics(self.raw_data[ch], start, seg_len, n)
                        _, pN = self.compute_harmonics(self.raw_data[ch], start, seg_len, n)
                        a_list.append(aN); p_list.append(pN)

                # Assign single-frame stats
                self.band_pass_peak_to_peak[ch] = float(np.mean(bandpass_ptps)) if bandpass_ptps else 0.0
                self.band_pass_peak_to_peak_history[ch] = [self.band_pass_peak_to_peak[ch]]
                self.band_pass_peak_to_peak_times[ch] = [0.0]
                self.one_x_amps[ch] = [float(np.mean(one_x_amps_list)) if one_x_amps_list else 0.0]
                self.one_x_phases[ch] = [float(np.mean(one_x_phases_list)) if one_x_phases_list else 0.0]
                self.two_x_amps[ch] = [float(np.mean(two_x_amps_list)) if two_x_amps_list else 0.0]
                self.two_x_phases[ch] = [float(np.mean(two_x_phases_list)) if two_x_phases_list else 0.0]
                # Store NX1/NX2/NX3 stats
                self.nx1_amps[ch] = [float(np.mean(nx1_amp_list)) if nx1_amp_list else 0.0]
                self.nx1_phases[ch] = [float(np.mean(nx1_phase_list)) if nx1_phase_list else 0.0]
                self.nx2_amps[ch] = [float(np.mean(nx2_amp_list)) if nx2_amp_list else 0.0]
                self.nx2_phases[ch] = [float(np.mean(nx2_phase_list)) if nx2_phase_list else 0.0]
                self.nx3_amps[ch] = [float(np.mean(nx3_amp_list)) if nx3_amp_list else 0.0]
                self.nx3_phases[ch] = [float(np.mean(nx3_phase_list)) if nx3_phase_list else 0.0]

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
        headers = list(self.internal_headers)
        try:
            for col, internal in enumerate(headers):
                item = QTableWidgetItem(channel_data[internal])
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
                subunit = (props.get("Subunit") or "pk-pk").lower()
                direct_values = [self._convert_ptp_by_subunit(np.ptp(self.raw_data[ch]), subunit)] if np.any(self.raw_data[ch]) else []
                channel_data = {
                    "Channel Name": channel_name,
                    "Unit": unit,
                    "DateTime": datetime.now().strftime("%d-%b-%Y %I:%M:%S %p"),
                    "RPM": f"{int(round(self.average_frequency[ch] * 60.0))}" if self.average_frequency[ch] > 0 else "0",
                    "Gap": (f"{float(self.gap_voltages[ch]):.2f}" if isinstance(self.gap_voltages, (list, tuple)) and ch < len(self.gap_voltages) and self.gap_voltages[ch] is not None else "0.00"),
                    "Direct": self.format_direct_bandpass_value(np.mean(direct_values) if direct_values else 0.0, unit),
                    "Bandpass": self.format_direct_bandpass_value(self._convert_ptp_by_subunit(self.band_pass_peak_to_peak[ch], subunit), unit),
                    "1xA": self.format_direct_value([np.mean(self.one_x_amps[ch])], unit) if self.one_x_amps[ch] else "0.00",
                    "1xP": f"{np.mean(self.one_x_phases[ch]):.0f}°" if self.one_x_phases[ch] else "0.00",
                    "2xA": self.format_direct_value([np.mean(self.two_x_amps[ch])], unit) if self.two_x_amps[ch] else "0.00",
                    "2xP": f"{np.mean(self.two_x_phases[ch]):.0f}°" if self.two_x_phases[ch] else "0.00",
                    "NXAmp": self.format_direct_value([np.mean(self.three_x_amps[ch])], unit) if self.three_x_amps[ch] else "0.00",
                    "NXPhase": f"{np.mean(self.three_x_phases[ch]):.0f}°" if self.three_x_phases[ch] else "0.00"
                }
                self.update_table_row(ch, channel_data)
            # After bulk updates, adjust rows and table height with throttling
            now = datetime.now()
            if (now - self._last_table_resize).total_seconds() >= self._table_resize_interval_sec:
                self.table.resizeRowsToContents()
                self.adjust_table_height()
                self._last_table_resize = now
            # Do not update plots (disabled)
            if self.console and (datetime.now() - self._last_log_time).total_seconds() >= self._log_interval_sec:
                self.console.append_to_console(f"Updated display for all {self.num_channels} channels")
                self._last_log_time = datetime.now()
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