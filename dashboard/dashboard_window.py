import sys
import gc
from PyQt5.QtWidgets import QWidget, QVBoxLayout, QSplitter, QSizePolicy, QApplication, QMessageBox
from PyQt5.QtCore import Qt, QTimer, pyqtSignal, QThread, QObject
from PyQt5.QtGui import QIcon, QColor
import os
import logging
from dashboard.components.file_bar import FileBar
from dashboard.components.tool_bar import ToolBar
from dashboard.components.sub_tool_bar import SubToolBar
from dashboard.components.main_section import MainSection
from dashboard.components.frequencyplot import FrequencyPlot
from dashboard.components.tree_view import TreeView
from dashboard.components.console import Console
from dashboard.components.mqtt_status import MQTTStatus
from mqtthandler import MQTTHandler
from features.tabular_view import TabularViewFeature
from features.polar import PolarPlotFeature
from features.time_view import TimeViewFeature
from features.fft_view import FFTViewFeature
from features.waterfall import WaterfallFeature
from features.centerline import CenterLineFeature
from features.orbit import OrbitFeature
from features.trend_view import TrendViewFeature
from features.multi_trend import MultiTrendFeature
from features.bode_plot import BodePlotFeature
from features.history_plot import HistoryPlotFeature
from features.time_report import TimeReportFeature
from features.report import ReportFeature
from select_project import SelectProjectWidget
from create_project import CreateProjectWidget
from project_structure import ProjectStructureWidget
import time
import re
from datetime import datetime

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class Worker(QObject):
    finished = pyqtSignal()
    select_project = pyqtSignal()

    def __init__(self, dashboard):
        super().__init__()
        self.dashboard = dashboard

    def run(self):
        try:
            projects = self.dashboard.db.load_projects()
            if projects and self.dashboard.current_project:
                self.dashboard.load_project(self.dashboard.current_project)
            else:
                self.select_project.emit()
        except Exception as e:
            logging.error(f"Error in deferred initialization: {str(e)}")
            self.dashboard.console.append_to_console(f"Error in deferred initialization: {str(e)}")
        finally:
            self.finished.emit()

class DashboardWindow(QWidget):
    mqtt_status_changed = pyqtSignal(bool)
    project_changed = pyqtSignal(str)
    saving_state_changed = pyqtSignal(bool)

    def __init__(self, db, email, auth_window=None):
        super().__init__()
        self.db = db
        self.email = email
        self.auth_window = auth_window

        self.current_project = None
        self.channel_count = None
        self.open_dashboards = {}
        self.current_feature = None
        self.mqtt_handler = None
        self.feature_instances = {}
        self.sub_windows = {}
        self.timer = QTimer(self)
        self.timer.setSingleShot(True)
        self.is_saving = False
        self.mqtt_connected = False
        self.select_project_widget = None
        self.create_project_widget = None
        self.project_structure_widget = None
        self.saving_filenames = {}
        self.last_selection_payload_by_model = {}
        self._freqplot_key = None
        self.selected_channel = None  # Store the currently selected channel from TreeView
        # Debounce maps to collapse rapid updates per feature instance
        self._debounce_timers = {}
        self._debounce_payloads = {}

        self.initUI()
        self.deferred_initialization()

    def initUI(self):
        self.setWindowTitle('Sarayu Desktop Application')
        self.setWindowState(Qt.WindowMaximized)
        # Set window icon using robust path resolution
        try:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            # __file__ here is in dashboard/, go one level up to project root
            root_dir = os.path.dirname(base_dir)
            candidates = [
                os.path.join(root_dir, 'logo.ico'),
                os.path.join(root_dir, 'logo.png'),
                os.path.join(root_dir, 'icons', 'placeholder.png'),
            ]
            icon_path = next((p for p in candidates if os.path.exists(p)), None)
            if icon_path:
                self.setWindowIcon(QIcon(icon_path))
        except Exception:
            pass
        app = QApplication.instance()
        app.setStyleSheet("""
        QInputDialog, QMessageBox {
            background-color: #1e2937;
            color: white;
            font-size: 16px;
            border: 1px solid #2c3e50;
            border-radius: 8px;
            padding: 15px;
            width:500px;
        }
        QInputDialog QLineEdit {
            background-color: #2c3e50;
            color: white;
            border: 1px solid #4a90e2;
            padding: 8px;
            border-radius: 4px;
            font-size: 15px;
        }
        QInputDialog QLabel,
        QMessageBox QLabel {
            color: #ecf0f1;
            font-size: 16px;
            padding-bottom: 10px;
        }
        QInputDialog QPushButton,
        QMessageBox QPushButton {
            background-color: #4a90e2;
            color: white;
            border: none;
            padding: 8px 16px;
            border-radius: 5px;
            font-size: 15px;
            min-width: 80px;
            transition: background-color 0.2s ease;
        }
        QInputDialog QPushButton:hover,
        QMessageBox QPushButton:hover {
            background-color: #357abd;
        }
        QInputDialog QPushButton:pressed,
        QMessageBox QPushButton:pressed {
            background-color: #2c5d9b;
        }
        QMdiSubWindow {
            background-color: #ebeef2;
            border: 1px solid #ebeef2;
            border-radius: 4px;
        }
        QMdiSubWindow > QWidget {
            background-color: #ebeef2;
            color: #ecf0f1;
        }
        """)

        main_layout = QVBoxLayout()
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)
        self.setLayout(main_layout)

        self.file_bar = FileBar(self)
        self.file_bar.home_triggered.connect(self.display_dashboard)
        self.file_bar.open_triggered.connect(self.open_project)
        self.file_bar.edit_triggered.connect(self.edit_project_dialog)
        self.file_bar.new_triggered.connect(self.create_project)
        self.file_bar.save_triggered.connect(self.save_action)
        self.file_bar.settings_triggered.connect(self.settings_action)
        self.file_bar.refresh_triggered.connect(self.refresh_action)
        self.file_bar.exit_triggered.connect(self.close)
        main_layout.addWidget(self.file_bar)

        self.tool_bar = ToolBar(self)
        self.tool_bar.feature_selected.connect(self.display_feature_content)
        main_layout.addWidget(self.tool_bar)

        self.sub_tool_bar = SubToolBar(self)
        self.sub_tool_bar.start_saving_triggered.connect(self.start_saving)
        self.sub_tool_bar.stop_saving_triggered.connect(self.stop_saving)
        self.sub_tool_bar.connect_mqtt_triggered.connect(self.connect_mqtt)
        self.sub_tool_bar.disconnect_mqtt_triggered.connect(self.disconnect_mqtt)
        self.sub_tool_bar.open_file_triggered.connect(self.handle_open_file)

        central_widget = QWidget()
        central_layout = QVBoxLayout()
        central_layout.setContentsMargins(0, 0, 0, 0)
        central_layout.setSpacing(0)
        central_widget.setLayout(central_layout)
        main_layout.addWidget(central_widget, 1)

        self.main_splitter = QSplitter(Qt.Horizontal)
        self.main_splitter.setContentsMargins(0, 0, 0, 0)
        self.main_splitter.setHandleWidth(1)
        self.main_splitter.setStyleSheet("QSplitter::handle { background-color: #2c3e50; }")
        central_layout.addWidget(self.main_splitter)

        self.tree_view = TreeView(self)
        self.tree_view.setVisible(False)
        self.tree_view.channel_selected.connect(self.on_channel_selected)
        self.main_splitter.addWidget(self.tree_view)

        right_container = QWidget()
        right_container.setStyleSheet("background-color: #ebeef2;")
        right_layout = QVBoxLayout()
        right_layout.setContentsMargins(0, 0, 0, 0)
        right_layout.setSpacing(0)
        right_container.setLayout(right_layout)

        self.sub_tool_bar.setVisible(False)
        right_layout.addWidget(self.sub_tool_bar)

        self.main_section = MainSection(self)
        right_layout.addWidget(self.main_section, 1)
        self.main_splitter.addWidget(right_container)

        window_width = self.width() if self.width() > 0 else 1200
        tree_view_width = int(window_width * 0.15)
        right_container_width = int(window_width * 0.85)
        self.main_splitter.setSizes([tree_view_width, right_container_width])

        self.console = Console(self)
        self.mqtt_status = MQTTStatus(self)

        self.console_layout = QVBoxLayout()
        self.console_layout.setContentsMargins(0, 0, 0, 0)
        self.console_layout.setSpacing(0)

        self.console_container = QWidget()
        self.console_container.setStyleSheet("background-color: black;")
        self.console_container.setFixedHeight(80)
        self.console_container.setLayout(self.console_layout)

        self.console_layout.addWidget(self.console.button_container)
        self.console_layout.addWidget(self.console.console_message_area)
        self.console_layout.addWidget(self.mqtt_status)

        main_layout.addWidget(self.console_container)

    def on_channel_selected(self, model_name, channel_name):
        """Handle channel selection from TreeView."""
        self.selected_channel = channel_name
        logging.debug(f"Channel selected: {model_name}/{channel_name}")
        self.console.append_to_console(f"Selected channel: {channel_name} for model {model_name}")

    def deferred_initialization(self):
        self.worker = Worker(self)
        self.thread = QThread()
        self.worker.moveToThread(self.thread)
        self.thread.started.connect(self.worker.run)
        self.worker.finished.connect(self.thread.quit)
        self.worker.finished.connect(self.worker.deleteLater)
        self.thread.finished.connect(self.thread.deleteLater)
        self.worker.select_project.connect(self.display_select_project)
        self.thread.start()

    def display_dashboard(self):
        self.clear_content_layout()
        self.tree_view.setVisible(False)
        self.sub_tool_bar.setVisible(False)
        self.current_project = None
        self.channel_count = None
        self.selected_channel = None
        self.file_bar.update_state(project_name=None)
        self.project_changed.emit(None)
        self.setWindowTitle('Sarayu Desktop Application')
        self.select_project_widget = SelectProjectWidget(self)
        self.main_section.set_widget(self.select_project_widget)
        logging.debug("Displayed dashboard with SelectProjectWidget in MainSection")

    def display_select_project(self):
        self.clear_content_layout()
        self.tree_view.setVisible(False)
        self.sub_tool_bar.setVisible(False)
        self.current_project = None
        self.channel_count = None
        self.selected_channel = None
        self.file_bar.update_state(project_name=None)
        self.project_changed.emit(None)
        self.setWindowTitle('Sarayu Desktop Application')
        self.select_project_widget = SelectProjectWidget(self)
        self.main_section.set_widget(self.select_project_widget)
        logging.debug("Displayed SelectProjectWidget in MainSection")

    def open_project(self):
        self.display_select_project()
        logging.debug("Opened project selection via SelectProjectWidget")

    def display_create_project(self):
        self.clear_content_layout()
        self.sub_tool_bar.setVisible(False)
        self.create_project_widget = CreateProjectWidget(self)
        self.main_section.set_widget(self.create_project_widget)
        logging.debug("Displayed CreateProjectWidget in MainSection")

    def edit_project_dialog(self):
        if not self.current_project:
            QMessageBox.warning(self, "Error", "No project selected to edit!")
            logging.warning("Attempted to edit project with no project selected")
            return

        self.clear_content_layout()
        self.tree_view.setVisible(False)
        self.sub_tool_bar.setVisible(False)

        project_data = self.db.get_project_data(self.current_project)
        if not project_data:
            self.console.append_to_console(f"Error: Project {self.current_project} not found.")
            logging.error(f"Project {self.current_project} not found!")
            self.display_select_project()
            return

        self.create_project_widget = CreateProjectWidget(
            self,
            edit_mode=True,
            existing_project_name=self.current_project,
            existing_models=project_data.get("models", []),
            existing_channel_count=project_data.get("channel_count", "DAQ4CH")
        )
        self.create_project_widget.project_edited.connect(self.handle_project_edited)
        self.main_section.set_widget(self.create_project_widget)
        logging.debug(f"Displayed CreateProjectWidget in edit mode for project: {self.current_project}")

    def handle_project_edited(self, project_name, models, channel_count):
        try:
            if not self.db.is_connected():
                self.db.reconnect()
            # Use the correct database API: edit_project(old_name, new_name, updated_models, channel_count)
            old_name = self.current_project
            # Capture currently open features (feature, model, channel) before updating the project
            previously_open = []
            try:
                for key in list(self.feature_instances.keys()):
                    feat_name, mdl_name, ch_name, _uid = key
                    previously_open.append((feat_name, mdl_name, ch_name))
            except Exception:
                previously_open = []
            was_mqtt_connected = bool(self.mqtt_connected)
            success, message = self.db.edit_project(old_name, project_name, updated_models=models, channel_count=channel_count)
            if success:
                QMessageBox.information(self, "Success", f"Project '{project_name}' updated successfully!")
                logging.info(f"Updated project: {project_name} with {len(models)} models")
                # Update current project reference and reload
                self.load_project(project_name)
                # Reopen all previously open features (same model/channel) so they re-read latest settings
                for feat_name, mdl_name, ch_name in previously_open:
                    try:
                        self.display_feature_for(feat_name, mdl_name, ch_name)
                    except Exception as e:
                        logging.error(f"Error reopening feature {feat_name} for {mdl_name}/{ch_name or 'No Channel'}: {e}")
                # Restore MQTT connection if it was active before edit so live plots resume
                if was_mqtt_connected:
                    try:
                        self.setup_mqtt()
                    except Exception:
                        logging.error("Failed to restore MQTT connection after project edit")
            else:
                QMessageBox.warning(self, "Error", message)
                logging.error(f"Failed to update project: {message}")
        except Exception as e:
            logging.error(f"Error updating project: {str(e)}")
            QMessageBox.warning(self, "Error", f"Failed to update project: {str(e)}")

    def display_feature_for(self, feature_name: str, model_name: str, channel_name: str = None):
        """Open a feature window for a specific model and channel, reusing display logic but honoring explicit model/channel.
        This is used after project edits to restore previously open features with updated settings.
        """
        if not self.current_project:
            QMessageBox.warning(self, "Error", "No project selected!")
            return
        project_data = self.db.get_project_data(self.current_project)
        if not project_data or "models" not in project_data:
            QMessageBox.warning(self, "Error", "No models found for the project.")
            return
        model = next((m for m in project_data["models"] if m.get("name") == model_name), None)
        if not model:
            QMessageBox.warning(self, "Error", f"Model '{model_name}' not found in project.")
            return
        channel_names = [ch.get("channelName") for ch in model.get("channels", [])]
        if feature_name in [
            "Time View", "Time Report", "Tabular View", "Multiple Trend View",
            "Waterfall", "Orbit", "Bode Plot", "Centerline"
        ]:
            channels_to_open = [None]
        else:
            ch = channel_name if channel_name in channel_names else (channel_names[0] if channel_names else None)
            channels_to_open = [ch]

        feature_classes = {
            "Tabular View": TabularViewFeature,
            "Time View": TimeViewFeature,
            "Time Report": TimeReportFeature,
            "FFT": FFTViewFeature,
            "Waterfall": WaterfallFeature,
            "Centerline": CenterLineFeature,
            "Orbit": OrbitFeature,
            "Trend View": TrendViewFeature,
            "Multiple Trend View": MultiTrendFeature,
            "Bode Plot": BodePlotFeature,
            "History Plot": HistoryPlotFeature,
            "Polar Plot": PolarPlotFeature,
            "Report": ReportFeature
        }
        if feature_name not in feature_classes:
            QMessageBox.warning(self, "Error", f"Unknown feature: {feature_name}")
            return
        for ch in channels_to_open:
            # Avoid duplicating if already open
            existing_key = next((k for k in self.feature_instances.keys() if k[0] == feature_name and k[1] == model_name and k[2] == ch), None)
            if existing_key:
                try:
                    sw = self.sub_windows.get(existing_key)
                    if sw:
                        sw.show(); sw.raise_(); sw.activateWindow();
                        if sw.isMinimized():
                            sw.showNormal()
                except Exception:
                    pass
                continue
            unique_id = int(time.time() * 1000)
            key = (feature_name, model_name, ch, unique_id)
            feature_kwargs = {
                "parent": self,
                "db": self.db,
                "project_name": self.current_project,
                "channel": ch,
                "model_name": model_name,
                "console": self.console
            }
            if feature_name in ["Orbit", "FFT", "Waterfall"]:
                feature_kwargs["channel_count"] = self.channel_count
            instance = feature_classes[feature_name](**feature_kwargs)
            self.feature_instances[key] = instance
            if self.mqtt_handler:
                self.mqtt_handler.add_active_feature(feature_name, model_name, ch)
            widget = instance.get_widget()
            if widget:
                sw = self.main_section.add_subwindow(widget, feature_name, channel_name=ch, model_name=model_name)
                if sw:
                    self.sub_windows[key] = sw
                    sw.closeEvent = lambda event, k=key: self.on_subwindow_closed(event, k)
                    sw.show()
            # If a frame was previously selected for this model, apply it so plots reflect updated settings
            payload = self.last_selection_payload_by_model.get(model_name)
            if payload and hasattr(instance, "load_selected_frame"):
                try:
                    instance.load_selected_frame(payload)
                except Exception as e:
                    logging.error(f"Error applying selected frame to {feature_name} after edit: {e}")
            self.current_feature = feature_name

    def create_project(self):
        self.display_create_project()
        logging.debug("Triggered create project action")

    def display_project_structure(self):
        self.clear_content_layout()
        self.tree_view.setVisible(False)
        self.sub_tool_bar.setVisible(False)
        self.project_structure_widget = ProjectStructureWidget(self)
        self.project_structure_widget.project_selected.connect(self.load_project)
        self.main_section.set_widget(self.project_structure_widget)
        self.main_splitter.setSizes([0, 1200])
        logging.debug("Displayed ProjectStructureWidget in MainSection")

    def load_project(self, project_name):
        # Ensure any existing MQTT connection is stopped before switching projects
        try:
            self.cleanup_mqtt()
        except Exception:
            pass
        self.current_project = project_name
        # Reset any stale selections/state so updates apply globally
        self.selected_channel = None
        self.last_selection_payload_by_model = {}
        project_data = self.db.get_project_data(project_name)
        if not project_data:
            self.console.append_to_console(f"Error: Project {project_name} not found.")
            logging.error(f"Project {project_name} not found!")
            self.display_select_project()
            return

        channel_count_map = {
            "DAQ4CH": 4,
            "DAQ8CH": 8,
            "DAQ10CH": 10
        }

        raw_channel_count = project_data.get("channel_count", 4)
        try:
            if isinstance(raw_channel_count, str):
                raw_norm = str(raw_channel_count).strip().upper().replace(" ", "").replace("_", "")
                # Try direct known keys
                self.channel_count = channel_count_map.get(raw_norm)
                if self.channel_count is None:
                    # Extract digits like '10' from 'DAQ10CH'
                    m = re.search(r"(\d+)", raw_norm)
                    if m:
                        self.channel_count = int(m.group(1))
                    else:
                        # Last resort
                        self.channel_count = int(raw_norm)
            else:
                self.channel_count = int(raw_channel_count)
            if self.channel_count not in [4, 8, 10]:
                raise ValueError(f"Invalid channel count: {self.channel_count}")
        except (ValueError, TypeError) as e:
            self.console.append_to_console(f"Error: Invalid channel count {raw_channel_count} for project {project_name}. Defaulting to 4.")
            logging.error(f"Invalid channel count {raw_channel_count} for project {project_name}: {str(e)}. Defaulting to 4.")
            self.channel_count = 4

        self.setWindowTitle(f'Sarayu Desktop Application - {self.current_project.upper()}')
        self.tree_view.setVisible(True)
        self.sub_tool_bar.setVisible(True)

        window_width = self.width() if self.width() > 0 else 1200
        tree_view_width = int(window_width * 0.15)
        right_container_width = int(window_width * 0.85)
        self.main_splitter.setSizes([tree_view_width, right_container_width])

        logging.debug(f"TreeView visibility: {self.tree_view.isVisible()}")
        logging.debug(f"SubToolBar visibility: {self.sub_tool_bar.isVisible()}")
        logging.debug(f"Loading project: {project_name} with {self.channel_count} channels")
        self.console.append_to_console(f"Loaded project {project_name} with {self.channel_count} channels")

        self.clear_content_layout()
        if self.project_structure_widget:
            self.project_structure_widget.setParent(None)
            self.project_structure_widget = None
            logging.debug("ProjectStructureWidget removed from MainSection")

        self.file_bar.update_state(project_name=project_name)
        self.project_changed.emit(project_name)
        self.load_project_features()
        # Do not auto-connect to MQTT; wait for explicit user action via Connect button
        try:
            self.console.append_to_console("MQTT is idle. Click 'Connect to MQTT' (🔗) to start streaming.")
        except Exception:
            pass

    def setup_mqtt(self):
        if not self.current_project:
            logging.warning("No project selected for MQTT setup")
            self.console.append_to_console("No project selected for MQTT setup")
            return

        self.cleanup_mqtt()
        try:
            tags = self.get_project_tags()
            if tags:
                self.mqtt_handler = MQTTHandler(self.db, self.current_project)
                self.mqtt_handler.data_received.connect(self.on_data_received)
                self.mqtt_handler.connection_status.connect(self.on_mqtt_status)
                self.mqtt_handler.save_status.connect(self.console.append_to_console)
                self.mqtt_handler.start()
                logging.info(f"MQTT setup initiated for project: {self.current_project}")
                self.console.append_to_console(f"MQTT setup initiated for project: {self.current_project}")
            else:
                logging.warning(f"No tags found for project: {self.current_project}")
                self.mqtt_connected = False
                self.mqtt_status_changed.emit(False)
                self.console.append_to_console(f"No tags found for project: {self.current_project}")
        except Exception as e:
            logging.error(f"Failed to setup MQTT: {str(e)}")
            self.console.append_to_console(f"Failed to setup MQTT: {str(e)}")
            self.mqtt_connected = False
            self.mqtt_status_changed.emit(False)

    def cleanup_mqtt(self):
        if self.mqtt_handler:
            try:
                self.mqtt_handler.data_received.disconnect()
                self.mqtt_handler.connection_status.disconnect()
                self.mqtt_handler.save_status.disconnect()
                self.mqtt_handler.stop()
                self.mqtt_handler.deleteLater()
                logging.info("Previous MQTT handler stopped")
            except Exception as e:
                logging.error(f"Error stopping MQTT handler: {str(e)}")
            finally:
                self.mqtt_handler = None
                self.mqtt_connected = False
                self.mqtt_status_changed.emit(False)

    def get_project_tags(self):
        try:
            if not self.db.is_connected():
                self.db.reconnect()
            project_data = self.db.get_project_data(self.current_project)
            if not project_data or "models" not in project_data:
                logging.warning(f"No models found for project: {self.current_project}")
                return []
            tags = []
            for model in project_data["models"]:
                model_name = model.get("name")
                tag_name = model.get("tagName", "")
                if tag_name and model_name:
                    tags.append({"tag_name": tag_name, "model_name": model_name})
            logging.debug(f"Retrieved tags for project {self.current_project}: {tags}")
            return tags
        except Exception as e:
            logging.error(f"Failed to retrieve project tags: {str(e)}")
            return []

    def on_data_received(self, feature_name, tag_name, model_name, channel_name, values, sample_rate, frame_index):
        try:
            for key, feature_instance in self.feature_instances.items():
                instance_feature, instance_model, instance_channel, _ = key
                if instance_model != model_name or feature_name not in self.mqtt_handler.feature_mapping:
                    continue
                mapped_features = self.mqtt_handler.feature_mapping[feature_name]
                if instance_feature not in mapped_features and instance_feature != feature_name:
                    continue

                # Features that expect all channels at once
                if instance_feature in [
                    "Time View",
                    "Time Report",
                    "Tabular View",
                    "Trend View",
                    "Multiple Trend View",
                    "Waterfall",
                    "Orbit",
                    "Bode Plot",
                    "Centerline"
                ]:
                    if channel_name is None:
                        # Route all-channel payloads
                        if instance_feature == "Trend View":
                            # Trend View instances are per-channel but need the full set of channels
                            dkey = (instance_feature, instance_model, instance_channel, id(feature_instance))
                            self._schedule_feature_update(dkey, instance_feature, instance_model, instance_channel,
                                                          feature_instance, tag_name, values, sample_rate, frame_index)
                        elif instance_channel is None:
                            # Other all-channel features have instance_channel None
                            dkey = (instance_feature, instance_model, instance_channel, id(feature_instance))
                            self._schedule_feature_update(dkey, instance_feature, instance_model, instance_channel,
                                                          feature_instance, tag_name, values, sample_rate, frame_index)
                else:
                    # Per-channel features: only route when MQTT provided a channel_name and it matches the instance channel
                    if channel_name is None:
                        continue
                    if instance_channel is None or instance_channel == channel_name:
                        dkey = (instance_feature, instance_model, channel_name, id(feature_instance))
                        self._schedule_feature_update(dkey, instance_feature, instance_model, channel_name,
                                                      feature_instance, tag_name, values, sample_rate, frame_index)
            logging.debug(f"Processed data for {feature_name}/{model_name}, frame {frame_index}, channel={channel_name or 'ALL'}")
        except Exception as e:
            logging.error(f"Error in on_data_received for {feature_name}/{model_name}, frame {frame_index}: {str(e)}")
            self.console.append_to_console(f"Error processing data for {feature_name}: {str(e)}")

    def _schedule_feature_update(self, dkey, feature_name, model_name, channel, feature_instance, tag_name, values, sample_rate, frame_index):
        """Debounce updates per feature instance key, keeping only the latest payload within a short window."""
        try:
            # Save latest payload for this key
            self._debounce_payloads[dkey] = (feature_name, model_name, channel, feature_instance, tag_name, values, sample_rate, frame_index)
            timer = self._debounce_timers.get(dkey)
            if not timer:
                timer = QTimer(self)
                timer.setSingleShot(True)
                # Use small debounce window to collapse bursts
                timer.setInterval(50)
                def fire(dk=dkey):
                    payload = self._debounce_payloads.pop(dk, None)
                    if payload:
                        f, m, ch, inst, t, v, sr, fi = payload
                        self._update_feature(f, m, ch, inst, t, v, sr, fi)
                    # Timer will be recreated lazily next time
                    self._debounce_timers.pop(dk, None)
                timer.timeout.connect(fire)
                self._debounce_timers[dkey] = timer
            # Restart timer to debounce
            timer.start()
        except Exception as e:
            logging.error(f"Error scheduling feature update: {e}")

    def _update_feature(self, feature_name, model_name, channel, feature_instance, tag_name, values, sample_rate, frame_index):
        try:
            if hasattr(feature_instance, 'on_data_received'):
                try:
                    # Preferred signature: (tag_name, model_name, values, sample_rate, frame_index)
                    feature_instance.on_data_received(tag_name, model_name, values, sample_rate, frame_index)
                except TypeError:
                    try:
                        # Some features (e.g., Bode Plot) expect feature_name as first arg
                        feature_instance.on_data_received(feature_name, tag_name, model_name, values, sample_rate, frame_index)
                    except TypeError:
                        # Backward-compat signature: (tag_name, model_name, values, sample_rate)
                        feature_instance.on_data_received(tag_name, model_name, values, sample_rate)
                logging.debug(f"Updated {feature_name} for {model_name}/{channel or 'all channels'}, frame {frame_index}")
        except Exception as e:
            logging.error(f"Error updating {feature_name} for {model_name}/{channel or 'all channels'}: {str(e)}")
            self.console.append_to_console(f"Error updating {feature_name}: {str(e)}")

    def load_project_features(self):
        # TreeView exposes update_project to (re)load models/channels for a project
        try:
            self.tree_view.update_project(self.current_project)
        except AttributeError:
            # Fallback compatibility: older TreeView may have add_project_to_tree
            if hasattr(self.tree_view, 'add_project_to_tree'):
                self.tree_view.add_project_to_tree(self.current_project)
            else:
                logging.error("TreeView does not support project loading APIs (update_project/add_project_to_tree)")

    def on_mqtt_status(self, status):
        self.mqtt_connected = "Connected" in status
        self.mqtt_status_changed.emit(self.mqtt_connected)
        self.console.append_to_console(status)
        logging.info(status)

    def start_saving(self):
        if not self.mqtt_handler or not self.current_project:
            QMessageBox.warning(self, "Error", "MQTT not connected or no project selected.")
            return

        project_data = self.db.get_project_data(self.current_project)
        if not project_data or "models" not in project_data:
            QMessageBox.warning(self, "Error", "No models found for the project.")
            return

        model_names = [model["name"] for model in project_data["models"]]
        if not model_names:
            QMessageBox.warning(self, "Error", "No models available.")
            return

        selected_model = model_names[0]
        # Use filename from SubToolBar input; fallback to next suggested if empty
        try:
            input_name = getattr(self.sub_tool_bar, 'filename_edit', None)
            filename = input_name.text().strip() if input_name else None
        except Exception:
            filename = None
        if not filename:
            # Fallback: simple timestamped default
            filename = f"data_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        self.mqtt_handler.start_saving(selected_model, filename)
        self.saving_filenames[selected_model] = filename
        self.is_saving = True
        self.saving_state_changed.emit(True)
        self.console.append_to_console(f"Started saving for model {selected_model} to {filename}")

    def stop_saving(self):
        if not self.saving_filenames:
            QMessageBox.warning(self, "Error", "No saving in progress.")
            return

        model_names = list(self.saving_filenames.keys())
        selected_model = model_names[0]
        # Preserve filename for user message before clearing
        saved_filename = self.saving_filenames.get(selected_model)

        try:
            if self.mqtt_handler:
                self.mqtt_handler.stop_saving(selected_model)
        except Exception as e:
            logging.error(f"Error stopping save for {selected_model}: {e}")
        del self.saving_filenames[selected_model]
        self.is_saving = bool(self.saving_filenames)
        self.saving_state_changed.emit(self.is_saving)
        # Inform the user precisely which file was saved
        if saved_filename:
            msg = f"Saved file as: {saved_filename}"
            try:
                QMessageBox.information(self, "Saved", msg)
            except Exception:
                pass
            self.console.append_to_console(msg)
        else:
            self.console.append_to_console(f"Stopped saving for model {selected_model}")
        # Advance filename field to next available name
        try:
            if hasattr(self.sub_tool_bar, 'refresh_filename'):
                self.sub_tool_bar.refresh_filename()
            if hasattr(self.sub_tool_bar, 'schedule_files_combo_update'):
                self.sub_tool_bar.schedule_files_combo_update()
        except Exception:
            pass

    def save_action(self):
        if self.current_project:
            try:
                if not self.db.is_connected():
                    self.db.reconnect()
                project_data = self.db.get_project_data(self.current_project)
                if project_data:
                    QMessageBox.information(self, "Save", f"Data for project '{self.current_project}' saved successfully!")
                else:
                    QMessageBox.warning(self, "Save Error", "No data to save for the selected project!")
            except Exception as e:
                logging.error(f"Error saving project: {str(e)}")
                QMessageBox.warning(self, "Error", f"Error saving project: {str(e)}")
        else:
            QMessageBox.warning(self, "Save Error", "No project selected to save!")

    def refresh_action(self):
        try:
            if self.current_project and self.current_feature:
                self.display_feature_content(self.current_feature)
                QMessageBox.information(self, "Refresh", f"Refreshed view for '{self.current_feature}'!")
            else:
                self.display_select_project()
                QMessageBox.information(self, "Refresh", "Refreshed project selection view!")
        except Exception as e:
            logging.error(f"Error refreshing view: {str(e)}")
            QMessageBox.warning(self, "Error", f"Error refreshing view: {str(e)}")

    def clear_content_layout(self):
        try:
            logging.debug("Starting clear_content_layout")
            for key in list(self.sub_windows.keys()):
                sub_window = self.sub_windows.get(key)
                if sub_window:
                    try:
                        if sub_window.isMaximized():
                            sub_window.showNormal()
                        sub_window.close()
                        self.main_section.mdi_area.removeSubWindow(sub_window)
                        sub_window.setParent(None)
                        sub_window.deleteLater()
                        logging.debug(f"Closed subwindow for {key} during clear_content_layout")
                    except Exception as e:
                        logging.error(f"Error closing subwindow {key}: {str(e)}")
            self.sub_windows.clear()
            logging.debug("Cleared all subwindows")

            for key in list(self.feature_instances.keys()):
                try:
                    instance = self.feature_instances[key]
                    if hasattr(instance, 'cleanup'):
                        instance.cleanup()
                        logging.debug(f"Called cleanup for feature instance {key}")
                    widget = instance.get_widget()
                    if widget:
                        widget.hide()
                        widget.setParent(None)
                        widget.deleteLater()
                        logging.debug(f"Cleaned up widget for {key}")
                    del self.feature_instances[key]
                    logging.debug(f"Removed feature instance for {key}")
                except Exception as e:
                    logging.error(f"Error cleaning up feature instance {key}: {str(e)}")

            self.main_section.clear_widget()
            self.main_section.mdi_area.setMinimumSize(0, 0)
            self.main_section.mdi_area.update()
            self.main_section.scroll_area.viewport().update()
            gc.collect()
            logging.debug("Completed clear_content_layout")
        except Exception as e:
            logging.error(f"Error clearing content layout: {str(e)}")

    def settings_action(self):
        QMessageBox.information(self, "Settings", "Settings functionality not implemented yet.")

    def back_to_login(self):
        try:
            if self.auth_window:
                self.auth_window.show()
                self.auth_window.showMaximized()
                self.close()
        except Exception as e:
            logging.error(f"Error returning to login: {str(e)}")
            QMessageBox.warning(self, "Error", f"Failed to return to login: {str(e)}")

    def closeEvent(self, event):
        try:
            if self.timer.isActive():
                self.timer.stop()
            self.cleanup_mqtt()
            self.clear_content_layout()
            if hasattr(self, 'thread') and self.thread.isRunning():
                self.thread.quit()
                self.thread.wait()
            if self.db and self.db.is_connected():
                self.db.close_connection()
            app = QApplication.instance()
            if app:
                app.quit()
        except Exception as e:
            logging.error(f"Error during closeEvent: {str(e)}")
        finally:
            event.accept()

    def resizeEvent(self, event):
        super().resizeEvent(event)
        if self.tree_view.isVisible():
            window_width = self.width()
            tree_view_width = int(window_width * 0.15)
            right_container_width = int(window_width * 0.85)
            self.main_splitter.setSizes([tree_view_width, right_container_width])
        self.main_section.arrange_layout()

    def connect_mqtt(self):
        if self.mqtt_connected:
            self.console.append_to_console("Already connected to MQTT")
            return
        QTimer.singleShot(0, self.setup_mqtt)

    def disconnect_mqtt(self):
        if not self.mqtt_connected:
            self.console.append_to_console("Already disconnected from MQTT")
            return
        try:
            self.cleanup_mqtt()
            self.mqtt_connected = False
            self.mqtt_status_changed.emit(False)
            logging.info(f"MQTT disconnected for project: {self.current_project}")
            self.console.append_to_console(f"MQTT disconnected for project: {self.current_project}")
        except Exception as e:
            logging.error(f"Failed to disconnect MQTT: {str(e)}")
            self.console.append_to_console(f"Failed to disconnect MQTT: {str(e)}")

    def display_feature_content(self, feature_name):
        if not self.current_project:
            QMessageBox.warning(self, "Error", "No project selected!")
            return

        project_data = self.db.get_project_data(self.current_project)
        if not project_data or "models" not in project_data:
            QMessageBox.warning(self, "Error", "No models found for the project.")
            return

        model_names = [model["name"] for model in project_data["models"]]
        if not model_names:
            QMessageBox.warning(self, "Error", "No models available.")
            return

        selected_model = model_names[0]
        model = next((m for m in project_data["models"] if m["name"] == selected_model), None)
        channel_names = [ch["channelName"] for ch in model.get("channels", [])] if model else []

        if not channel_names:
            QMessageBox.warning(self, "Error", "No channels available for the model.")
            return
        current_console_height = self.console.console_message_area.height()
        self.console.console_message_area.setFixedHeight(current_console_height)

        try:
            # Determine which channels to open for this feature
            if feature_name in [
                "Time View",
                "Time Report",
                "Tabular View",
                "Multiple Trend View",
                "Waterfall",
                "Orbit",
                "Bode Plot",
                "Centerline"
            ]:
                channel_list = [None]
            else:
                if self.selected_channel and self.selected_channel in channel_names:
                    channel_list = [self.selected_channel]
                else:
                    channel_list = [channel_names[0]]
                    self.console.append_to_console(f"No channel selected in TreeView. Defaulting to {channel_names[0]}.")
                    logging.debug(f"No channel selected. Defaulting to {channel_names[0]} for {feature_name}")

            feature_classes = {
                "Tabular View": TabularViewFeature,
                "Time View": TimeViewFeature,
                "Time Report": TimeReportFeature,
                "FFT": FFTViewFeature,
                "Waterfall": WaterfallFeature,
                "Centerline": CenterLineFeature,
                "Orbit": OrbitFeature,
                "Trend View": TrendViewFeature,
                "Multiple Trend View": MultiTrendFeature,
                "Bode Plot": BodePlotFeature,
                "History Plot": HistoryPlotFeature,
                "Polar Plot": PolarPlotFeature,
                "Report": ReportFeature
            }

            if feature_name not in feature_classes:
                logging.warning(f"Unknown feature: {feature_name}")
                QMessageBox.warning(self, "Error", f"Unknown feature: {feature_name}")
                return

            opened_new = False
            for channel in channel_list:
                existing_key = None
                for key in self.feature_instances.keys():
                    if key[0] == feature_name and key[1] == selected_model and key[2] == channel:
                        existing_key = key
                        break

                if existing_key:
                    sub_window = self.sub_windows.get(existing_key)
                    if sub_window:
                        try:
                            sub_window.show()
                            sub_window.raise_()
                            sub_window.activateWindow()
                            if sub_window.isMinimized():
                                sub_window.showNormal()
                            logging.debug(f"Activated existing subwindow for {feature_name}/{selected_model}/{channel or 'No Channel'}")
                            self.console.append_to_console(f"{feature_name} already open. Brought to front.")
                        except Exception as e:
                            logging.error(f"Error activating existing subwindow for {existing_key}: {str(e)}")
                    continue

                unique_id = int(time.time() * 1000)
                key = (feature_name, selected_model, channel, unique_id)
                try:
                    if not self.db.is_connected():
                        self.db.reconnect()

                    feature_kwargs = {
                        "parent": self,
                        "db": self.db,
                        "project_name": self.current_project,
                        "channel": channel,
                        "model_name": selected_model,
                        "console": self.console
                    }
                    if feature_name in ["Orbit", "FFT", "Waterfall"]:
                        feature_kwargs["channel_count"] = self.channel_count

                    feature_instance = feature_classes[feature_name](**feature_kwargs)

                    if feature_name == "Tabular View":
                        logging.debug(f"TabularViewFeature initialized for model {selected_model}, channel {channel or 'None'}; displays all {self.channel_count} channels")
                    else:
                        logging.debug(f"Initialized {feature_name} for model {selected_model}, channel {channel or 'None'}")

                    if feature_name in ["Orbit", "FFT"] and channel and hasattr(feature_instance, 'update_selected_channel'):
                        feature_instance.update_selected_channel(channel)

                    self.feature_instances[key] = feature_instance
                    if self.mqtt_handler:
                        self.mqtt_handler.add_active_feature(feature_name, selected_model, channel)
                    widget = feature_instance.get_widget()
                    if widget:
                        sub_window = self.main_section.add_subwindow(
                            widget,
                            feature_name,
                            channel_name=channel,
                            model_name=selected_model
                        )
                        if sub_window:
                            self.sub_windows[key] = sub_window
                            sub_window.closeEvent = lambda event, k=key: self.on_subwindow_closed(event, k)
                            sub_window.show()
                            logging.debug(f"Created new subwindow for {key}")
                            opened_new = True
                        else:
                            logging.error(f"Failed to create subwindow for {feature_name}/{selected_model}/{channel or 'No Channel'}")
                            QMessageBox.warning(self, "Error", f"Failed to create subwindow for {feature_name}")
                            del self.feature_instances[key]
                            if self.mqtt_handler:
                                self.mqtt_handler.remove_active_feature(feature_name, selected_model, channel)
                    else:
                        logging.error(f"Feature {feature_name} returned invalid widget")
                        QMessageBox.warning(self, "Error", f"Feature {feature_name} failed to initialize")
                        del self.feature_instances[key]
                        if self.mqtt_handler:
                            self.mqtt_handler.remove_active_feature(feature_name, selected_model, channel)

                    payload = self.last_selection_payload_by_model.get(selected_model)
                    if payload and hasattr(feature_instance, "load_selected_frame"):
                        try:
                            feature_instance.load_selected_frame(payload)
                            self.console.append_to_console(f"{feature_name}: loaded frame {payload.get('frameIndex')} from {payload.get('filename')}")
                        except Exception as e:
                            self.console.append_to_console(f"{feature_name}: error loading selected frame: {e}")

                    self.console.console_message_area.setFixedHeight(current_console_height)
                except Exception as e:
                    logging.error(f"Failed to load feature {feature_name} for channel {channel or 'No Channel'}: {str(e)}")
                    QMessageBox.warning(self, "Error", f"Failed to load {feature_name}: {str(e)}")
                    if key in self.feature_instances:
                        del self.feature_instances[key]
                    if self.mqtt_handler:
                        self.mqtt_handler.remove_active_feature(feature_name, selected_model, channel)

            if opened_new:
                self.main_section.arrange_layout()
                self.console.console_message_area.setFixedHeight(current_console_height)
            else:
                self.console.append_to_console(f"{feature_name} is already open.")
            self.current_feature = feature_name  # Update current_feature
        except Exception as e:
            logging.error(f"Error displaying feature content: {str(e)}")
            QMessageBox.warning(self, "Error", f"Error displaying feature: {str(e)}")

    def handle_open_file(self, file_data):
        try:
            self.clear_content_layout()

            freq_plot = FrequencyPlot(
                parent=self,
                project_name=file_data["project_name"],
                model_name=file_data["model_name"],
                filename=file_data["filename"],
                email=self.email
            )
            freq_plot.time_range_selected.connect(self.on_frequency_selection)

            sub_window = self.main_section.add_subwindow(
                freq_plot,
                "Frequency Plot",
                model_name=file_data["model_name"],
                channel_name=file_data["filename"]
            )
            if sub_window:
                self._freqplot_key = ("Frequency Plot", file_data["model_name"], file_data["filename"], id(freq_plot))
                self.sub_windows[self._freqplot_key] = sub_window
                sub_window.closeEvent = lambda event, k=self._freqplot_key: self.on_subwindow_closed(event, k)
                sub_window.show()
                try:
                    sub_window.showMaximized()
                except Exception:
                    pass
                self.main_section.arrange_layout()
                logging.debug(f"Opened FrequencyPlot for {file_data}")
                self.console.append_to_console(f"Opened FrequencyPlot for {file_data['filename']} (model: {file_data['model_name']})")
            else:
                logging.error(f"Failed to open FrequencyPlot subwindow for {file_data}")
                self.console.append_to_console("Failed to open Frequency Plot window")
        except Exception as e:
            logging.error(f"Error handling open file: {str(e)}")
            QMessageBox.warning(self, "Error", f"Failed to open file: {str(e)}")

    def on_frequency_selection(self, selected_payload: dict):
        try:
            model_name = selected_payload.get("model")
            if not self.current_project or not model_name:
                self.console.append_to_console("Project or model missing for selection.")
                return

            # Normalize payload keys for all features
            normalized = dict(selected_payload)
            # Provide both 'channelData' and 'message' for compatibility
            cd = normalized.get("channelData")
            if cd is None and isinstance(normalized.get("message"), list):
                cd = normalized.get("message")
                normalized["channelData"] = cd
            if normalized.get("message") is None and isinstance(cd, list):
                normalized["message"] = cd
            # Ensure common meta fields
            if "numberOfChannels" not in normalized and "num_main" in normalized:
                normalized["numberOfChannels"] = normalized.get("num_main")
            if "tacoChannelCount" not in normalized and "num_tacho" in normalized:
                normalized["tacoChannelCount"] = normalized.get("num_tacho")
            if "samplingRate" not in normalized and "Fs" in normalized:
                normalized["samplingRate"] = normalized.get("Fs")
            if "samplingSize" not in normalized and "N" in normalized:
                normalized["samplingSize"] = normalized.get("N")

            self.last_selection_payload_by_model[model_name] = normalized
            self.console.append_to_console(
                f"Selected frame {normalized.get('frameIndex')} from {normalized.get('filename')} "
                f"stored for model {model_name}. Now choose a feature to view."
            )

            if self._freqplot_key and self._freqplot_key in self.sub_windows:
                try:
                    sw = self.sub_windows.get(self._freqplot_key)
                    if sw:
                        if sw.isMaximized():
                            sw.showNormal()
                        sw.close()
                        self.main_section.mdi_area.removeSubWindow(sw)
                        sw.setParent(None)
                        sw.deleteLater()
                    del self.sub_windows[self._freqplot_key]
                    self._freqplot_key = None
                    self.main_section.arrange_layout()
                except Exception as e:
                    logging.error(f"Error closing FrequencyPlot window after selection: {e}")

            # Apply selected frame to features and auto-open defaults if needed
            self._apply_selected_frame_to_features(model_name)
        except Exception as e:
            logging.error(f"Failed to handle frequency selection: {str(e)}")
            self.console.append_to_console(f"Error applying selection: {str(e)}")

    def on_subwindow_closed(self, event, key):
        try:
            feature_name, model_name, channel_name, unique_id = key
            logging.debug(f"Closing subwindow for key: {key}")

            sub_window = self.sub_windows.get(key)
            if not sub_window:
                logging.warning(f"No subwindow found for key: {key}")
                event.accept()
                return

            if sub_window.isMaximized():
                sub_window.showNormal()
                logging.debug(f"Restored maximized subwindow for {key}")

            if key in self.feature_instances:
                instance = self.feature_instances[key]
                if hasattr(instance, 'cleanup'):
                    try:
                        instance.cleanup()
                        logging.debug(f"Called cleanup for {feature_name}/{model_name}/{channel_name or 'No Channel'}")
                    except Exception as e:
                        logging.error(f"Error in cleanup for {key}: {str(e)}")
                widget = instance.get_widget()
                if widget:
                    try:
                        widget.hide()
                        widget.setParent(None)
                        widget.deleteLater()
                        logging.debug(f"Cleaned up widget for {key}")
                    except Exception as e:
                        logging.error(f"Error cleaning up widget for {key}: {str(e)}")
                del self.feature_instances[key]
                logging.debug(f"Removed feature instance for {key}")

            if self.mqtt_handler:
                self.mqtt_handler.remove_active_feature(feature_name, model_name, channel_name)

            try:
                sub_window.close()
                self.main_section.mdi_area.removeSubWindow(sub_window)
                sub_window.setParent(None)
                sub_window.deleteLater()
                logging.debug(f"Removed subwindow from MDI area for {key}")
            except Exception as e:
                logging.error(f"Error removing subwindow for {key}: {str(e)}")
            del self.sub_windows[key]

            if self.current_feature == feature_name:
                if not any(k[0] == feature_name for k in self.feature_instances.keys()):
                    self.current_feature = None
                    self.is_saving = bool(self.saving_filenames)
                    self.saving_state_changed.emit(self.is_saving)
                    logging.debug(f"Reset current_feature as no instances of {feature_name} remain")

            self.main_section.mdi_area.update()
            self.main_section.scroll_area.viewport().update()
            self.main_section.arrange_layout()
            self.main_section.mdi_area.setMinimumSize(0, 0)
            gc.collect()
            logging.debug(f"Completed cleanup for subwindow: {key}")
        except Exception as e:
            logging.error(f"Error cleaning up subwindow for {key}: {str(e)}")

    def _apply_selected_frame_to_features(self, model_name: str):
        try:
            payload = self.last_selection_payload_by_model.get(model_name)
            if not payload:
                return
            # Update already-open features for this model
            updated_count = 0
            for key, instance in list(self.feature_instances.items()):
                try:
                    feature_name, k_model, k_channel, _uid = key
                    if k_model != model_name:
                        continue
                    if hasattr(instance, "load_selected_frame"):
                        instance.load_selected_frame(payload)
                        updated_count += 1
                        logging.debug(f"Applied selected frame to {feature_name} for model {model_name}")
                except Exception as e:
                    logging.error(f"Error applying selected frame to {key}: {e}")
            # Do not auto-open any features; rely on user-selected subwindows only
            self.console.append_to_console(f"Applied selected frame to {updated_count} open feature(s) for model {model_name}.")
        except Exception as e:
            logging.error(f"Error in _apply_selected_frame_to_features: {e}")