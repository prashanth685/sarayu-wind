
from PyQt5.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QFormLayout, QLineEdit, QPushButton, QLabel, QMessageBox, QScrollArea, QComboBox, QApplication, QTableWidget, QTableWidgetItem, QHeaderView
from PyQt5.QtCore import Qt, pyqtSignal
import sys
import datetime
import logging

app = QApplication(sys.argv)

# 🌐 Global stylesheet for QMessageBox and QComboBox
app.setStyleSheet("""
    QMessageBox {
        background-color: #fefefe;
        color: #1a202c;
        font: 13px "Segoe UI";
        border: 1px solid #cbd5e0;
        padding: 10px;
    }

    QMessageBox QLabel {
        color: #1a202c;
    }

    QMessageBox QPushButton {
        background-color: #3b82f6;
        color: white;
        border: none;
        padding: 6px 12px;
        border-radius: 4px;
        min-width: 80px;
        font-weight: 500;
    }

    QMessageBox QPushButton:hover {
        background-color: #2563eb;
    }

    QMessageBox QPushButton:pressed {
        background-color: #1d4ed8;
    }

    QComboBox {
        border: 1px solid #d1d5db;
        border-radius: 4px;
        padding: 6px;
        font-size: 13px;
        background-color: #ffffff;
        min-height: 28px;
    }
    QComboBox::drop-down {
        border-left: 1px solid #d1d5db;
        width: 20px;
    }
    QComboBox::down-arrow {
        image: none;
        width: 10px;
        height: 10px;
    }
    QComboBox:hover {
        border-color: #93c5fd;
    }
    QComboBox:focus {
        border-color: #3b82f6;
        outline: none;
    }
""")

class CreateProjectWidget(QWidget):
    project_edited = pyqtSignal(str, list, str)  # Signal for edited project (new_project_name, updated_models, channel_count)

    def __init__(self, parent=None, edit_mode=False, existing_project_name=None, existing_models=None, existing_channel_count="DAQ4CH"):
        super().__init__(parent)
        self.parent = parent
        self.db = parent.db
        self.edit_mode = edit_mode
        self.existing_project_name = existing_project_name
        self.existing_models = existing_models or []
        self.existing_channel_count = existing_channel_count
        self.models = []
        self.available_types = ["Displacement", "Acc/Vel"]
        self.available_directions = ["Right", "Left"]
        self.available_channel_counts = ["DAQ4CH", "DAQ8CH", "DAQ10CH"]
        self.available_units_displacement = ["mil", "mm", "um"]
        self.available_units_accvel = ["g", "m/s²", "mm/s"]
        self.initUI()
        logging.debug(f"Initialized CreateProjectWidget in {'edit' if edit_mode else 'create'} mode for project: {existing_project_name}")

    def initUI(self):
        self.setStyleSheet("background-color: #f7f7f9;")

        main_layout = QVBoxLayout()
        main_layout.setAlignment(Qt.AlignCenter)
        main_layout.setSpacing(20)
        main_layout.setContentsMargins(40, 20, 40, 20)
        self.setLayout(main_layout)

        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setStyleSheet("""
            QScrollArea {
                border: none;
                background-color: transparent;
            }
        """)
        main_layout.addWidget(scroll_area)

        scroll_widget = QWidget()
        scroll_layout = QVBoxLayout()
        scroll_layout.setAlignment(Qt.AlignCenter)
        scroll_layout.setSpacing(24)
        scroll_widget.setLayout(scroll_layout)
        scroll_area.setWidget(scroll_widget)

        card_widget = QWidget()
        card_widget.setStyleSheet("""
            QWidget {
                background-color: white;
                border-radius: 8px;
                box-shadow: 0 2px 10px rgba(0, 0, 0, 0.05);
                padding: 24px;
            }
        """)
        card_layout = QVBoxLayout()
        card_layout.setSpacing(16)
        card_widget.setLayout(card_layout)
        scroll_layout.addWidget(card_widget)

        title_label = QLabel("Edit Project" if self.edit_mode else "Create New Project")
        title_label.setStyleSheet("""
            font-size: 20px;
            font-weight: 600;
            color: #1a202c;
            margin-bottom: 8px;
        """)
        card_layout.addWidget(title_label, alignment=Qt.AlignCenter)

        subtitle_label = QLabel("Modify project details and models" if self.edit_mode else "Start by defining project details and models")
        subtitle_label.setStyleSheet("""
            font-size: 14px;
            color: #6b7280;
            margin-bottom: 16px;
        """)
        card_layout.addWidget(subtitle_label, alignment=Qt.AlignCenter)

        project_details_label = QLabel("Project Details")
        project_details_label.setStyleSheet("""
            font-size: 16px;
            font-weight: 500;
            color: #1a202c;
            margin-top: 16px;
            margin-bottom: 8px;
        """)
        card_layout.addWidget(project_details_label)

        project_form = QFormLayout()
        project_form.setSpacing(12)
        project_form.setLabelAlignment(Qt.AlignLeft)
        project_form.setFormAlignment(Qt.AlignCenter)
        self.project_name_input = QLineEdit()
        self.project_name_input.setPlaceholderText("Project name")
        if self.edit_mode and self.existing_project_name:
            self.project_name_input.setText(self.existing_project_name)
        self.project_name_input.setStyleSheet("""
            QLineEdit {
                border: 1px solid #d1d5db;
                border-radius: 4px;
                padding: 8px;
                font-size: 14px;
                min-width: 400px;
                background-color: #ffffff;
            }
            QLineEdit:focus {
                border-color: #3b82f6;
                outline: none;
            }
            QLineEdit:hover {
                border-color: #93c5fd;
            }
        """)
        project_form.addRow("Project Name:", self.project_name_input)

        self.channel_count_combo = QComboBox()
        self.channel_count_combo.addItems(self.available_channel_counts)
        if self.edit_mode and self.existing_channel_count:
            self.channel_count_combo.setCurrentText(self.existing_channel_count)
        self.channel_count_combo.setStyleSheet("""
            QComboBox {
                border: 1px solid #d1d5db;
                border-radius: 4px;
                padding: 8px;
                font-size: 14px;
                min-width: 400px;
                background-color: #ffffff;
            }
            QComboBox:focus {
                border-color: #3b82f6;
                outline: none;
            }
            QComboBox:hover {
                border-color: #93c5fd;
            }
        """)
        self.channel_count_combo.currentTextChanged.connect(self.update_table)
        project_form.addRow("Channel Count:", self.channel_count_combo)
        card_layout.addLayout(project_form)

        add_model_button = QPushButton("+ Add Model")
        add_model_button.setStyleSheet("""
            QPushButton {
                background-color: #3b82f6;
                color: white;
                border-radius: 4px;
                padding: 8px;
                font-size: 14px;
                font-weight: 500;
                min-width: 120px;
            }
            QPushButton:hover {
                background-color: #2563eb;
            }
            QPushButton:pressed {
                background-color: #1d4ed8;
            }
        """)
        add_model_button.clicked.connect(self.add_model_input)
        card_layout.addWidget(add_model_button, alignment=Qt.AlignRight)

        self.model_layout = QVBoxLayout()
        self.model_layout.setSpacing(16)
        self.model_inputs = []
        card_layout.addLayout(self.model_layout)

        # Pre-populate models if in edit mode
        if self.edit_mode and self.existing_models:
            for model in self.existing_models:
                self.add_model_input(model)

        button_layout = QHBoxLayout()
        button_layout.setSpacing(12)
        button_layout.setAlignment(Qt.AlignLeft)

        back_button = QPushButton("Back")
        back_button.setStyleSheet("""
            QPushButton {
                background-color: transparent;
                color: #6b7280;
                border: 1px solid #d1d5db;
                border-radius: 4px;
                padding: 8px;
                font-size: 14px;
                font-weight: 500;
                min-width: 100px;
            }
            QPushButton:hover {
                background-color: #f1f5f9;
            }
            QPushButton:pressed {
                background-color: #e2e8f0;
            }
        """)
        back_button.clicked.connect(self.back_to_select)
        button_layout.addWidget(back_button)

        create_button = QPushButton("Update Project" if self.edit_mode else "Create Project")
        create_button.setStyleSheet("""
            QPushButton {
                background-color: #3b82f6;
                color: white;
                border-radius: 4px;
                padding: 8px;
                font-size: 14px;
                font-weight: 500;
                min-width: 120px;
            }
            QPushButton:hover {
                background-color: #2563eb;
            }
            QPushButton:pressed {
                background-color: #1d4ed8;
            }
        """)
        create_button.clicked.connect(self.submit_project)
        button_layout.addWidget(create_button)

        card_layout.addLayout(button_layout)

    def update_table(self, channel_count):
        for widget, model_name_input, tag_name_input, channel_inputs, _ in self.model_inputs:
            for table, num_channels in channel_inputs:
                model_layout = widget.layout()
                model_layout.removeWidget(table)
                table.deleteLater()

            num_channels = {"DAQ4CH": 4, "DAQ8CH": 8, "DAQ10CH": 10}.get(channel_count, 4)
            table = QTableWidget(num_channels, 11)
            table.setHorizontalHeaderLabels(["S.No.", "Channel Name", "Channel Type", "Sensitivity", "Unit", "Correction Factor", "Gain", "Unit Type", "Angle", "Direction", "Shaft"])
            table.setStyleSheet("""
                QTableWidget {
                    border: 1px solid #e5e7eb;
                    border-radius: 8px;
                    background-color: #ffffff;
                    padding: 8px;
                    font-size: 13px;
                }
                QTableWidget::item {
                    padding: 8px;
                    border-bottom: 1px solid #f1f5f9;
                    color: #2d3748;
                    font-size: 13px;
                }
                QTableWidget::item:selected {
                    background-color: #edf2f7;
                    color: #2d3748;
                }
                QHeaderView::section {
                    background-color: #4a5568;
                    color: white;
                    padding: 8px;
                    font-weight: 600;
                    border: none;
                    border-bottom: 2px solid #2d3748;
                    font-size: 13px;
                    min-height: 32px;
                }
            """)
            table.horizontalHeader().setVisible(True)
            table.horizontalHeader().setStretchLastSection(True)
            table.horizontalHeader().setMinimumHeight(36)
            table.verticalHeader().setVisible(False)
            table.setAlternatingRowColors(True)
            table.setEditTriggers(QTableWidget.AllEditTriggers)
            table.setMinimumHeight(table.rowHeight(0) * num_channels + table.horizontalHeader().height() + 20)
            table.setMaximumHeight(table.rowHeight(0) * num_channels + table.horizontalHeader().height() + 20)
            table.resizeColumnsToContents()
            table.setMinimumWidth(800)

            for row in range(num_channels):
                item = QTableWidgetItem(str(row + 1))
                item.setTextAlignment(Qt.AlignCenter)
                table.setItem(row, 0, item)
                table.setItem(row, 1, QTableWidgetItem(""))
                
                type_combo = QComboBox()
                type_combo.addItems(self.available_types)
                type_combo.setCurrentText("Displacement")
                type_combo.currentIndexChanged.connect(lambda _, r=row: self.update_unit_combo(table, r))
                table.setCellWidget(row, 2, type_combo)
                
                table.setItem(row, 3, QTableWidgetItem(""))
                
                unit_combo = QComboBox()
                unit_combo.addItems(self.available_units_displacement)
                unit_combo.setCurrentText("mil")
                table.setCellWidget(row, 4, unit_combo)
                
                table.setItem(row, 5, QTableWidgetItem(""))
                table.setItem(row, 6, QTableWidgetItem(""))
                table.setItem(row, 7, QTableWidgetItem(""))
                table.setItem(row, 8, QTableWidgetItem(""))
                
                direction_combo = QComboBox()
                direction_combo.addItems(self.available_directions)
                direction_combo.setCurrentText("Right")
                table.setCellWidget(row, 9, direction_combo)
                
                table.setItem(row, 10, QTableWidgetItem(""))

            model_layout.addWidget(table)
            channel_inputs[0] = (table, num_channels)

    def update_unit_combo(self, table, row):
        type_combo = table.cellWidget(row, 2)
        unit_combo = table.cellWidget(row, 4)
        current_type = type_combo.currentText()
        unit_combo.clear()
        unit_items = self.available_units_displacement if current_type == "Displacement" else self.available_units_accvel
        unit_combo.addItems(unit_items)
        unit_combo.setCurrentText(unit_items[0])

    def add_model_input(self, existing_model=None):
        channel_count = self.channel_count_combo.currentText()
        num_channels = {"DAQ4CH": 4, "DAQ8CH": 8, "DAQ10CH": 10}.get(channel_count, 4)

        model_widget = QWidget()
        model_widget.setStyleSheet("""
            background-color: #fafafa;
            border-radius: 4px;
            padding: 16px;
            border: 1px solid #e5e7eb;
        """)
        model_layout = QVBoxLayout()
        model_layout.setSpacing(12)
        model_widget.setLayout(model_layout)

        model_header_layout = QHBoxLayout()
        model_header_layout.setSpacing(8)
        model_label = QLabel(f"Model {len(self.model_inputs) + 1}")
        model_label.setStyleSheet("""
            font-size: 16px;
            font-weight: 500;
            color: #1a202c;
        """)
        model_header_layout.addWidget(model_label)

        remove_model_button = QPushButton("Remove Model")
        remove_model_button.setStyleSheet("""
            QPushButton {
                background-color: transparent;
                color: #ef4444;
                border: none;
                font-size: 14px;
                font-weight: 500;
            }
            QPushButton:hover {
                color: #dc2626;
            }
            QPushButton:pressed {
                color: #b91c1c;
            }
        """)
        remove_model_button.clicked.connect(lambda: self.remove_model_input(model_widget))
        model_header_layout.addWidget(remove_model_button, alignment=Qt.AlignRight)
        model_layout.addLayout(model_header_layout)

        model_form = QFormLayout()
        model_form.setSpacing(12)
        model_form.setLabelAlignment(Qt.AlignLeft)
        model_form.setFormAlignment(Qt.AlignCenter)

        model_name_input = QLineEdit()
        model_name_input.setPlaceholderText("Model name")
        if existing_model:
            model_name = existing_model.get("name", "")
            if model_name.startswith(channel_count + "_"):
                model_name = model_name[len(channel_count) + 1:]
            model_name_input.setText(model_name)
        model_name_input.setStyleSheet("""
            QLineEdit {
                border: 1px solid #d1d5db;
                border-radius: 4px;
                padding: 8px;
                font-size: 14px;
                min-width: 400px;
                background-color: #ffffff;
            }
            QLineEdit:focus {
                border-color: #3b82f6;
                outline: none;
            }
            QLineEdit:hover {
                border-color: #93c5fd;
            }
        """)
        model_form.addRow("Model Name:", model_name_input)

        tag_name_input = QLineEdit()
        tag_name_input.setPlaceholderText("Tag name")
        if existing_model:
            tag_name_input.setText(existing_model.get("tagName", ""))
        tag_name_input.setStyleSheet("""
            QLineEdit {
                border: 1px solid #d1d5db;
                border-radius: 4px;
                padding: 8px;
                font-size: 14px;
                min-width: 400px;
                background-color: #ffffff;
            }
            QLineEdit:focus {
                border-color: #3b82f6;
                outline: none;
            }
            QLineEdit:hover {
                border-color: #93c5fd;
            }
        """)
        model_form.addRow("Tag Name:", tag_name_input)
        model_layout.addLayout(model_form)

        channels_label = QLabel("Channels")
        channels_label.setStyleSheet("""
            font-size: 14px;
            font-weight: 500;
            color: #1a202c;
            margin-top: 8px;
            margin-bottom: 8px;
        """)
        model_layout.addWidget(channels_label)

        table = QTableWidget(num_channels, 11)
        table.setHorizontalHeaderLabels(["S.No.", "Channel Name", "Channel Type", "Sensitivity", "Unit", "Correction Factor", "Gain", "Unit Type", "Angle", "Direction", "Shaft"])
        table.setStyleSheet("""
            QTableWidget {
                background-color: #ffffff;
                border: 1px solid #e5e7eb;
                border-radius: 6px;
                font-size: 13px;
                gridline-color: #e5e7eb;
                selection-background-color: #edf2f7;
                selection-color: #1a202c;
                alternate-background-color: #f9fafb;
            }
            QTableWidget::item {
                padding: 6px;
                border: none;
                height:70px;
                color: #1a202c;
            }
            QHeaderView::section {
                background-color: #4a5568;
                color: white;
                height:70px;
                font-weight: 600;
                font-size: 13px;
                border: none;
                border-bottom: 1px solid #e5e7eb;
            }
        """)
        table.horizontalHeader().setVisible(True)
        table.horizontalHeader().setStretchLastSection(True)
        table.horizontalHeader().setMinimumHeight(45)
        table.verticalHeader().setVisible(False)
        table.setAlternatingRowColors(True)
        table.setEditTriggers(QTableWidget.AllEditTriggers)
        table.setSelectionBehavior(QTableWidget.SelectRows)
        table.setSelectionMode(QTableWidget.SingleSelection)
        table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        table.setMinimumHeight(table.rowHeight(0) * num_channels + table.horizontalHeader().height() + 20)
        table.setMaximumHeight(table.rowHeight(0) * num_channels + table.horizontalHeader().height() + 20)
        table.resizeColumnsToContents()
        table.setMinimumWidth(800)

        if existing_model and existing_model.get("channels"):
            for row, channel in enumerate(existing_model["channels"]):
                if row >= num_channels:
                    break
                item = QTableWidgetItem(str(row + 1))
                item.setTextAlignment(Qt.AlignCenter)
                table.setItem(row, 0, item)
                table.setItem(row, 1, QTableWidgetItem(channel.get("channelName", "")))
                
                type_combo = QComboBox()
                type_combo.addItems(self.available_types)
                type_combo.setCurrentText(channel.get("type", "Displacement"))
                type_combo.currentIndexChanged.connect(lambda _, r=row: self.update_unit_combo(table, r))
                table.setCellWidget(row, 2, type_combo)
                
                table.setItem(row, 3, QTableWidgetItem(channel.get("sensitivity", "")))
                
                unit_combo = QComboBox()
                current_type = type_combo.currentText()
                unit_items = self.available_units_displacement if current_type == "Displacement" else self.available_units_accvel
                unit_combo.addItems(unit_items)
                unit_combo.setCurrentText(channel.get("unit", unit_items[0]))
                table.setCellWidget(row, 4, unit_combo)
                
                table.setItem(row, 5, QTableWidgetItem(channel.get("correctionValue", "")))
                table.setItem(row, 6, QTableWidgetItem(channel.get("gain", "")))
                table.setItem(row, 7, QTableWidgetItem(channel.get("unitType", "")))
                table.setItem(row, 8, QTableWidgetItem(channel.get("angle", "")))
                
                direction_combo = QComboBox()
                direction_combo.addItems(self.available_directions)
                direction_combo.setCurrentText(channel.get("angleDirection", "Right"))
                table.setCellWidget(row, 9, direction_combo)
                
                table.setItem(row, 10, QTableWidgetItem(channel.get("shaft", "")))
        else:
            for row in range(num_channels):
                item = QTableWidgetItem(str(row + 1))
                item.setTextAlignment(Qt.AlignCenter)
                table.setItem(row, 0, item)
                table.setItem(row, 1, QTableWidgetItem(""))
                
                type_combo = QComboBox()
                type_combo.addItems(self.available_types)
                type_combo.setCurrentText("Displacement")
                type_combo.currentIndexChanged.connect(lambda _, r=row: self.update_unit_combo(table, r))
                table.setCellWidget(row, 2, type_combo)
                
                table.setItem(row, 3, QTableWidgetItem(""))
                
                unit_combo = QComboBox()
                unit_combo.addItems(self.available_units_displacement)
                unit_combo.setCurrentText("mil")
                table.setCellWidget(row, 4, unit_combo)
                
                table.setItem(row, 5, QTableWidgetItem(""))
                table.setItem(row, 6, QTableWidgetItem(""))
                table.setItem(row, 7, QTableWidgetItem(""))
                table.setItem(row, 8, QTableWidgetItem(""))
                
                direction_combo = QComboBox()
                direction_combo.addItems(self.available_directions)
                direction_combo.setCurrentText("Right")
                table.setCellWidget(row, 9, direction_combo)
                
                table.setItem(row, 10, QTableWidgetItem(""))

        model_layout.addWidget(table)

        self.model_inputs.append((model_widget, model_name_input, tag_name_input, [(table, num_channels)], channel_count))
        self.model_layout.addWidget(model_widget)

    def add_channel_to_table(self, table):
        current_rows = table.rowCount()
        table.setRowCount(current_rows + 1)
        item = QTableWidgetItem(str(current_rows + 1))
        item.setTextAlignment(Qt.AlignCenter)
        table.setItem(current_rows, 0, item)
        table.setItem(current_rows, 1, QTableWidgetItem(""))
        
        type_combo = QComboBox()
        type_combo.addItems(self.available_types)
        type_combo.setCurrentText("Displacement")
        type_combo.currentIndexChanged.connect(lambda _, r=current_rows: self.update_unit_combo(table, r))
        table.setCellWidget(current_rows, 2, type_combo)
        
        table.setItem(current_rows, 3, QTableWidgetItem(""))
        
        unit_combo = QComboBox()
        unit_combo.addItems(self.available_units_displacement)
        unit_combo.setCurrentText("mil")
        table.setCellWidget(current_rows, 4, unit_combo)
        
        table.setItem(current_rows, 5, QTableWidgetItem(""))
        table.setItem(current_rows, 6, QTableWidgetItem(""))
        table.setItem(current_rows, 7, QTableWidgetItem(""))
        table.setItem(current_rows, 8, QTableWidgetItem(""))
        
        direction_combo = QComboBox()
        direction_combo.addItems(self.available_directions)
        direction_combo.setCurrentText("Right")
        table.setCellWidget(current_rows, 9, direction_combo)
        
        table.setItem(current_rows, 10, QTableWidgetItem(""))
        table.setMinimumHeight(table.rowHeight(0) * (current_rows + 1) + table.horizontalHeader().height() + 20)
        table.setMaximumHeight(table.rowHeight(0) * (current_rows + 1) + table.horizontalHeader().height() + 20)
        table.resizeColumnsToContents()

    def remove_model_input(self, model_widget):
        if len(self.model_inputs) > 1 or not self.edit_mode:
            for inputs in self.model_inputs:
                if inputs[0] == model_widget:
                    self.model_inputs.remove(inputs)
                    self.model_layout.removeWidget(model_widget)
                    model_widget.deleteLater()
                    for i, (widget, _, _, _, _) in enumerate(self.model_inputs):
                        widget.layout().itemAt(0).layout().itemAt(0).widget().setText(f"Model {i + 1}")
                    break

    def submit_project(self):
        project_name = self.project_name_input.text().strip()
        channel_count = self.channel_count_combo.currentText()
        if not project_name:
            QMessageBox.warning(self, "Error", "Project name cannot be empty!")
            return

        if not self.model_inputs:
            QMessageBox.warning(self, "Error", "At least one model is required!")
            return

        self.models = []
        for _, model_name_input, tag_name_input, channel_inputs, _ in self.model_inputs:
            model_name = model_name_input.text().strip()
            tag_name = tag_name_input.text().strip()
            if not model_name:
                QMessageBox.warning(self, "Error", f"Model name cannot be empty for model {len(self.models) + 1}!")
                return

            channels = []
            for table, num_channels in channel_inputs:
                for row in range(table.rowCount()):
                    channel_name = table.item(row, 1).text().strip() if table.item(row, 1) else ""
                    if not channel_name:
                        QMessageBox.warning(self, "Error", f"Channel name cannot be empty for model '{model_name}'!")
                        return
                    channels.append({
                        "channelName": channel_name,
                        "type": table.cellWidget(row, 2).currentText() if table.cellWidget(row, 2) else "Displacement",
                        "sensitivity": table.item(row, 3).text().strip() if table.item(row, 3) else "",
                        "unit": table.cellWidget(row, 4).currentText() if table.cellWidget(row, 4) else "mil",
                        "correctionValue": table.item(row, 5).text().strip() if table.item(row, 5) else "",
                        "gain": table.item(row, 6).text().strip() if table.item(row, 6) else "",
                        "unitType": table.item(row, 7).text().strip() if table.item(row, 7) else "",
                        "angle": table.item(row, 8).text().strip() if table.item(row, 8) else "",
                        "angleDirection": table.cellWidget(row, 9).currentText() if table.cellWidget(row, 9) else "Right",
                        "shaft": table.item(row, 10).text().strip() if table.item(row, 10) else ""
                    })

            if not channels:
                QMessageBox.warning(self, "Error", f"At least one channel is required for model '{model_name}'!")
                return

            self.models.append({
                "name": f"{channel_count}_{model_name}",
                "tagName": tag_name,
                "channels": channels
            })

        try:
            if self.edit_mode:
                self.project_edited.emit(project_name, self.models, channel_count)
            else:
                success, message = self.db.create_project(project_name, self.models, channel_count)
                if success:
                    QMessageBox.information(self, "Success", "Project created successfully!")
                    logging.info(f"Created new project: {project_name} with {len(self.models)} models")
                    logging.debug(f"Calling load_project for project: {project_name}")
                    self.parent.load_project(project_name)
                else:
                    QMessageBox.warning(self, "Error", message)
        except Exception as e:
            logging.error(f"Error submitting project: {str(e)}")
            QMessageBox.warning(self, "Error", f"Failed to submit project: {str(e)}")

    def back_to_select(self):
        logging.debug("Returning to project selection UI")
        self.parent.display_select_project()    