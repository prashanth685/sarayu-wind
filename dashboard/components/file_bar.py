from PyQt5.QtWidgets import QToolBar, QAction, QWidget, QSizePolicy, QToolButton
from PyQt5.QtCore import Qt, pyqtSignal
import logging

class FileBar(QToolBar):
    # Signals to communicate with DashboardWindow
    home_triggered = pyqtSignal()
    open_triggered = pyqtSignal()
    edit_triggered = pyqtSignal()
    new_triggered = pyqtSignal()
    save_triggered = pyqtSignal()
    settings_triggered = pyqtSignal()
    refresh_triggered = pyqtSignal()
    exit_triggered = pyqtSignal()

    def __init__(self, parent):
        super().__init__("File", parent)
        self.parent = parent
        self.current_project = None
        self.mqtt_connected = False
        self.initUI()
        if hasattr(self.parent, 'mqtt_status_changed'):
            self.parent.mqtt_status_changed.connect(self.update_mqtt_status)

    def initUI(self):
        self.setStyleSheet("""
            QToolBar {
                background: #2D2F33;
                border: none;
                padding: 0;
                spacing: 5px;
            }
            QToolBar QToolButton {
                font-size: 18px;
                font-weight: bold;
                color: #fff;
                padding: 8px 12px;
                border-radius: 4px;
                background-color: transparent;
            }
            QToolBar QToolButton:hover {
                background-color: #4a90e2;
                color: white;
            }
            QToolBar QToolButton:disabled {
                color: #666;
            }
        """)
        self.setFixedHeight(40)
        self.setMovable(False)
        self.setFloatable(False)

        # Define actions
        self.actions = {
            "Home": QAction("Home", self),
            "Open": QAction("Open", self),
            "Edit": QAction("Edit", self),
            "New": QAction("New", self),
            "Save": QAction("Save", self),
            "Settings": QAction("Settings", self),
            "Refresh": QAction("Refresh", self),
        }

        action_configs = [
            ("Home", "Go to Dashboard Home", self.home_triggered),
            ("Open", "Open an Existing Project", self.open_triggered),
            ("Edit", "Edit an Existing Project", self.edit_triggered),
            ("New", "Create a New Project", self.new_triggered),
            ("Save", "Save Current Project Data", self.save_triggered),
            ("Settings", "Open Application Settings", self.settings_triggered),
            ("Refresh", "Refresh Current View", self.refresh_triggered),
        ]

        for action_name, tooltip, signal in action_configs:
            action = self.actions[action_name]
            action.setToolTip(tooltip)
            action.triggered.connect(signal.emit)
            self.addAction(action)

        # Add spacer to push Exit to the right
        spacer = QWidget()
        spacer.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        self.addWidget(spacer)

        # Add Exit action at far right
    def update_state(self, project_name=None, mqtt_connected=None):
        """Update action states and toolbar appearance based on application state."""
        try:
            if project_name is not None:
                self.current_project = project_name
            if mqtt_connected is not None:
                self.mqtt_connected = mqtt_connected

            always_enabled = ["Home", "Open", "New", "Settings"]
            for name in always_enabled:
                self.actions[name].setEnabled(True)

            project_dependent = ["Save", "Edit", "Refresh"]
            has_project = self.current_project is not None
            for name in project_dependent:
                self.actions[name].setEnabled(has_project)

            # Optional: dynamic background/text color based on project
            background = "#2D2F33" if has_project else "#f5f5f5"
            text_color = "#fff" if has_project else "#333"
            self.setStyleSheet(f"""
                QToolBar {{
                    background: {background};
                    border: none;
                    padding: 0;
                    spacing: 5px;
                }}
                QToolBar QToolButton {{
                    font-size: 18px;
                    font-weight: bold;
                    color: {text_color};
                    padding: 8px 12px;
                    border-radius: 4px;
                    background-color: transparent;
                }}
                QToolBar QToolButton:hover {{
                    background-color: #4a90e2;
                    color: white;
                }}
                QToolBar QToolButton:disabled {{
                    color: #666;
                }}
            """)

            logging.debug(f"FileBar updated: project={self.current_project}, mqtt_connected={self.mqtt_connected}")
        except Exception as e:
            logging.error(f"Error updating FileBar state: {str(e)}")

    def update_mqtt_status(self, connected):
        """Update MQTT connection status."""
        self.update_state(mqtt_connected=connected)
