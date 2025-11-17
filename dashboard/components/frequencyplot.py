from PyQt5.QtWidgets import QWidget, QVBoxLayout, QLabel, QPushButton, QSlider, QHBoxLayout, QMessageBox, QSizePolicy
from PyQt5.QtCore import Qt, pyqtSignal, QTimer
import pyqtgraph as pg
import numpy as np
import datetime
import logging
from database import Database

class FrequencyPlot(QWidget):
    time_range_selected = pyqtSignal(dict)

    def __init__(self, parent=None, project_name=None, model_name=None, filename=None, start_time=None, end_time=None, email="user@example.com"):
        super().__init__(parent)
        # Use a reasonable minimum size and let the widget expand with its container
        self.setMinimumSize(640, 480)
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.project_name = project_name
        self.model_name = model_name
        self.filename = filename
        self.start_time = self.parse_time(start_time) if start_time else None
        self.end_time = self.parse_time(end_time) if end_time else None
        self.email = email
        self.db = Database(connection_string="mongodb://localhost:27017/", email=email)

        self.current_records = []
        self.filtered_records = []
        self.lower_time_percentage = 0
        self.upper_time_percentage = 100
        self.time_data = None
        self.frequency_data = None

        self.selected_record = None
        self.is_crosshair_visible = False
        self.is_crosshair_locked = False
        self.locked_crosshair_position = None
        self.last_mouse_move = datetime.datetime.now()
        self.mouse_move_debounce_ms = 50

        self.debounce_timer = QTimer()
        self.debounce_timer.setSingleShot(True)
        self.debounce_timer.timeout.connect(self.filter_and_plot_data)
        self.debounce_delay = 200

        self.is_dragging_range = False
        self.drag_start_x = 0

        self.initUI()
        self.initialize_data()

    def parse_time(self, time_str):
        try:
            return datetime.datetime.fromisoformat(time_str.replace('Z', '+00:00'))
        except Exception as e:
            logging.error(f"Error parsing time {time_str}: {str(e)}")
            return None

    def initUI(self):
        self.layout = QVBoxLayout()
        self.layout.setContentsMargins(10, 10, 10, 10)
        self.layout.setSpacing(10)

        self.title_label = QLabel(f"Frequency Analysis for {self.filename}")
        self.title_label.setStyleSheet("font-size: 16px; font-weight: bold; color: #333;")
        self.layout.addWidget(self.title_label)

        # Create PyQtGraph plot widget
        self.plot_widget = pg.PlotWidget()
        self.plot_widget.setBackground('w')
        self.plot_widget.showGrid(x=True, y=True, alpha=0.3)
        self.plot_widget.setLabel('left', 'Frequency', units='Hz')
        self.plot_widget.setLabel('bottom', 'Time')
        self.plot_widget.setTitle('Tacho Frequency vs Time', size='14pt', bold=True)
        
        # Let the plot widget expand in the available space
        self.plot_widget.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.layout.addWidget(self.plot_widget, stretch=1)
        
        # Enable crosshair
        self.plot_widget.setMouseEnabled(x=True, y=True)
        self.vLine = pg.InfiniteLine(angle=90, movable=False)
        self.hLine = pg.InfiniteLine(angle=0, movable=False)
        self.plot_widget.addItem(self.vLine, ignoreBounds=True)
        self.plot_widget.addItem(self.hLine, ignoreBounds=True)
        
        # Proxy for crosshair
        self.proxy = pg.SignalProxy(self.plot_widget.scene().sigMouseMoved, rateLimit=60, slot=self.mouseMoved)
        self.plot_widget.scene().sigMouseClicked.connect(self.mouseClicked)

        self.slider_widget = QWidget()
        self.slider_layout = QHBoxLayout()
        self.slider_widget.setLayout(self.slider_layout)
        # Make the slider area responsive
        self.slider_widget.setMinimumHeight(44)
        self.slider_widget.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)

        self.start_label = QLabel("Start: ")
        self.start_label.setStyleSheet("font-size: 14px; color: #333;")
        self.slider_layout.addWidget(self.start_label)

        self.start_slider = QSlider(Qt.Horizontal)
        self.start_slider.setMinimum(0)
        self.start_slider.setMaximum(100)
        self.start_slider.setValue(0)
        self.start_slider.valueChanged.connect(self.update_labels)
        self.slider_layout.addWidget(self.start_slider)

        self.end_label = QLabel("End: ")
        self.end_label.setStyleSheet("font-size: 14px; color: #333;")
        self.slider_layout.addWidget(self.end_label)

        self.end_slider = QSlider(Qt.Horizontal)
        self.end_slider.setMinimum(0)
        self.end_slider.setMaximum(100)
        self.end_slider.setValue(100)
        self.end_slider.valueChanged.connect(self.update_labels)
        self.slider_layout.addWidget(self.end_slider)

        self.layout.addWidget(self.slider_widget)

        self.range_indicator = QPushButton("Drag Range")
        self.range_indicator.setStyleSheet("""
        QPushButton { background-color: #4a90e2; color: white; border: none; padding: 8px 16px; border-radius: 5px; font-size: 14px; }
        QPushButton:hover { background-color: #357abd; }
        QPushButton:pressed { background-color: #2c5d9b; }
        """)
        self.range_indicator.pressed.connect(self.start_range_drag)
        self.range_indicator.released.connect(self.stop_range_drag)
        self.slider_layout.addWidget(self.range_indicator)
        self.slider_widget.mouseMoveEvent = self.range_mouse_move

        self.select_button = QPushButton("Select")
        self.select_button.setStyleSheet("""
        QPushButton { background-color: #4a90e2; color: white; border: none; padding: 8px 16px; border-radius: 5px; font-size: 14px; }
        QPushButton:hover { background-color: #357abd; }
        QPushButton:pressed { background-color: #2c5d9b; }
        """)
        self.select_button.clicked.connect(self.select_button_click)
        self.slider_layout.addWidget(self.select_button)

        self.setLayout(self.layout)

    def initialize_data(self):
        try:
            messages = self.db.get_history_messages(self.project_name, self.model_name, filename=self.filename) 
            if not messages:
                logging.error(f"No history messages found for {self.filename}")
                return 

            self.current_records = sorted(messages, key=lambda x: x.get("frameIndex", 0))
            self.filtered_records = self.current_records.copy()

            # Extract time and frequency data from database records
            self.time_data = []
            self.frequency_data = []
            
            for record in self.current_records:
                # Use actual timestamp instead of frame index
                timestamp = self.parse_time(record.get("createdAt"))
                if timestamp is None:
                    # Fallback to frame index if timestamp is not available
                    timestamp = record.get("frameIndex", 0)
                else:
                    # Convert timestamp to numeric value (seconds since epoch)
                    timestamp = timestamp.timestamp()
                
                # Extract frequency from tacho channel data (similar to time_report.py approach)
                message = record.get("message", [])
                num_main_channels = record.get("numberOfChannels", 0)
                taco_channel_count = record.get("tacoChannelCount", 0)
                sampling_size = record.get("samplingSize", 0)
                
                # Tacho frequency data is stored after main channels in the flattened message
                if taco_channel_count > 0 and sampling_size > 0 and len(message) >= num_main_channels * sampling_size:
                    # Calculate start index of tacho frequency data
                    tacho_start = num_main_channels * sampling_size
                    tacho_end = min(tacho_start + sampling_size, len(message))
                    # Extract the entire tacho frequency channel and add ALL values (not just mean)
                    if tacho_start < len(message) and tacho_end > tacho_start:
                        tacho_freq_channel = message[tacho_start:tacho_end]
                        # Add each frequency value with its timestamp
                        base_time = timestamp
                        sample_rate = record.get("samplingRate", 1000)  # Default to 1000 Hz if not specified
                        time_step = 1.0 / float(sample_rate)
                        
                        for i, freq_val in enumerate(tacho_freq_channel):
                            self.time_data.append(base_time + i * time_step)
                            self.frequency_data.append(float(freq_val) if freq_val else 0)
                else:
                    # Fallback to messageFrequency if available
                    freq = record.get("messageFrequency", 0)
                    self.time_data.append(timestamp)
                    self.frequency_data.append(float(freq) if freq else 0)

            if not self.start_time and self.current_records:
                first_record = min(self.current_records, key=lambda x: (self.parse_time(x.get("createdAt")) or datetime.datetime.min).timestamp())
                self.start_time = self.parse_time(first_record.get("createdAt"))
            if not self.end_time and self.current_records:
                last_record = max(self.current_records, key=lambda x: (self.parse_time(x.get("createdAt")) or datetime.datetime.min).timestamp())
                self.end_time = self.parse_time(last_record.get("createdAt"))

            # Initial plot
            self.filter_and_plot_data()
        except Exception as e:
            logging.error(f"Error initializing: {str(e)}")
            import traceback
            logging.error(traceback.format_exc())

    def filter_and_plot_data(self):
        try:
            if not self.current_records or not self.time_data:
                return

            # Clear the plot
            self.plot_widget.clear()
            
            # Re-add crosshair lines after clearing
            self.vLine = pg.InfiniteLine(angle=90, movable=False)
            self.hLine = pg.InfiniteLine(angle=0, movable=False)
            self.plot_widget.addItem(self.vLine, ignoreBounds=True)
            self.plot_widget.addItem(self.hLine, ignoreBounds=True)

            # Use actual timestamps for filtering
            min_time = min(self.time_data) if self.time_data else 0
            max_time = max(self.time_data) if self.time_data else 0
            time_range = max(max_time - min_time, 1)  # Ensure at least 1 to avoid division by zero
            lower_time = min_time + (time_range * self.lower_time_percentage / 100.0)
            upper_time = min_time + (time_range * self.upper_time_percentage / 100.0)

            # Filter data based on time range
            filtered_times = []
            filtered_frequencies = []
            
            for i, timestamp in enumerate(self.time_data):
                if lower_time <= timestamp <= upper_time:
                    filtered_times.append(timestamp)
                    filtered_frequencies.append(self.frequency_data[i])
            
            if filtered_times and filtered_frequencies:
                # Convert to numpy arrays for plotting
                time_array = np.array(filtered_times)
                freq_array = np.array(filtered_frequencies)
                
                # Create plot item
                self.plot_widget.plot(time_array, freq_array, 
                                     pen=pg.mkPen('b', width=2),
                                     symbol='o',
                                     symbolSize=3,
                                     symbolBrush='b',
                                     name='Tacho Frequency')
                
                # Set axis labels and title
                self.plot_widget.setLabel('left', 'Frequency', units='Hz')
                self.plot_widget.setLabel('bottom', 'Time')
                self.plot_widget.setTitle('Tacho Frequency vs Time', size='14pt', bold=True)
                
                # Format x-axis to show time properly
                axis = self.plot_widget.getAxis('bottom')
                # Create string ticks from timestamps
                num_ticks = min(10, len(time_array))
                if num_ticks > 1:
                    tick_indices = np.linspace(0, len(time_array)-1, num_ticks, dtype=int)
                    tick_texts = [datetime.datetime.fromtimestamp(time_array[i]).strftime('%H:%M:%S') for i in tick_indices]
                    tick_positions = time_array[tick_indices]
                    axis.setTicks([list(zip(tick_positions, tick_texts))])
                
                # Update time range labels
                lower_time_str = datetime.datetime.fromtimestamp(lower_time).strftime('%H:%M:%S')
                upper_time_str = datetime.datetime.fromtimestamp(upper_time).strftime('%H:%M:%S')
                
                self.start_label.setText(f"Start: {lower_time_str}")
                self.end_label.setText(f"End: {upper_time_str}")
            else:
                self.start_label.setText("Start: --:--:--")
                self.end_label.setText("End: --:--:--")
        except Exception as e:
            logging.error(f"Error in filter_and_plot_data: {str(e)}")
            import traceback
            logging.error(traceback.format_exc())

    def update_labels(self):
        self.lower_time_percentage = self.start_slider.value()
        self.upper_time_percentage = self.end_slider.value()
        if self.time_data:
            min_time = min(self.time_data)
            max_time = max(self.time_data)
            time_range = max(max_time - min_time, 1)
            lower_time = min_time + (time_range * self.lower_time_percentage / 100.0)
            upper_time = min_time + (time_range * self.upper_time_percentage / 100.0)
            
            # Convert timestamps to readable format
            lower_time_str = datetime.datetime.fromtimestamp(lower_time).strftime('%H:%M:%S')
            upper_time_str = datetime.datetime.fromtimestamp(upper_time).strftime('%H:%M:%S')
            
            self.start_label.setText(f"Start: {lower_time_str}")
            self.end_label.setText(f"End: {upper_time_str}")
        else:
            self.start_label.setText("Start: --:--:--")
            self.end_label.setText("End: --:--:--")
        self.debounce_timer.start(self.debounce_delay)

    def mouseMoved(self, evt):
        """Handle mouse movement for crosshair"""
        if not self.is_crosshair_locked and evt:
            pos = evt[0]  # Get the mouse position
            if self.plot_widget.plotItem.vb.sceneBoundingRect().contains(pos):
                mousePoint = self.plot_widget.plotItem.vb.mapSceneToView(pos)
                x, y = mousePoint.x(), mousePoint.y()
                
                # Update crosshair lines
                self.vLine.setPos(x)
                self.hLine.setPos(y)
                
                # Update status with current values
                if self.time_data and self.frequency_data:
                    # Find nearest data point
                    idx = np.searchsorted(np.array(self.time_data), x)
                    if 0 <= idx < len(self.time_data):
                        time_str = datetime.datetime.fromtimestamp(self.time_data[idx]).strftime('%H:%M:%S')
                        freq = self.frequency_data[idx]
                        # You could update a status label here if needed
                        logging.debug(f"Time: {time_str}, Frequency: {freq:.2f} Hz")

    def mouseClicked(self, evt):
        """Handle mouse click for locking/unlocking crosshair"""
        if not evt:
            return
            
        pos = evt.scenePos()
        if self.plot_widget.plotItem.vb.sceneBoundingRect().contains(pos):
            mousePoint = self.plot_widget.plotItem.vb.mapSceneToView(pos)
            x, y = mousePoint.x(), mousePoint.y()
            
            if not self.is_crosshair_locked:
                self.is_crosshair_locked = True
                self.locked_crosshair_position = (x, y)
                logging.debug(f"Crosshair locked at ({x:.2f}, {y:.2f})")
            else:
                self.is_crosshair_locked = False
                self.locked_crosshair_position = None
                logging.debug("Crosshair unlocked")

    def start_range_drag(self):
        self.is_dragging_range = True
        if self.time_data:
            span = (self.time_data[-1] - self.time_data[0]) if len(self.time_data) > 1 else 1
            min_time = min(self.time_data)
            self.drag_start_x = min_time + span * (self.lower_time_percentage / 100.0)

    def stop_range_drag(self):
        self.is_dragging_range = False

    def range_mouse_move(self, event):
        if self.is_dragging_range and event is not None and hasattr(event, "x"):
            # For slider area we don't have data coords; ignore unless you want to map pixels
            pass

    def update_range_on_drag(self, x):
        if x is None or not self.time_data:
            return
        
        # Convert datetime to timestamp if needed
        if hasattr(x, 'timestamp'):
            x_numeric = x.timestamp()
        else:
            x_numeric = x
            
        denom = (self.time_data[-1] - self.time_data[0]) if len(self.time_data) > 1 else 1
        if denom == 0:
            return
        delta_x = x_numeric - self.drag_start_x
        delta_percentage = (delta_x / denom) * 100.0
        new_lower = max(0.0, min(100.0, self.lower_time_percentage + delta_percentage))
        new_upper = max(0.0, min(100.0, self.upper_time_percentage + delta_percentage))
        if new_lower < new_upper:
            self.lower_time_percentage = new_lower
            self.upper_time_percentage = new_upper
            self.start_slider.setValue(int(new_lower))
            self.end_slider.setValue(int(new_upper))
            self.filter_and_plot_data()

    def find_closest_record_by_timestamp(self, selected_timestamp):
        try:
            if not self.filtered_records:
                return None
            
            # Find the record with timestamp closest to the selected timestamp
            closest_record = None
            min_diff = float('inf')
            
            for i, record in enumerate(self.filtered_records):
                if i < len(self.time_data):
                    record_timestamp = self.time_data[i]
                    diff = abs(record_timestamp - selected_timestamp)
                    if diff < min_diff:
                        min_diff = diff
                        closest_record = record
            
            if closest_record and closest_record.get("message"):
                return closest_record
            
            # Fallback fetch full record if minimal doc
            query = {
                "filename": self.filename,
                "moduleName": self.model_name,
                "projectName": self.project_name,
                "frameIndex": closest_record.get("frameIndex"),
                "email": self.email
            }
            full_records = list(self.db.history_collection.find(query))
            if full_records:
                return full_records[0]
            return closest_record
        except Exception as e:
            logging.error(f"Error finding closest record by timestamp: {str(e)}")
            return None

    def find_closest_record(self, selected_frame_index):
        try:
            if not self.filtered_records:
                return None
            closest_record = min(self.filtered_records, key=lambda r: abs(r.get("frameIndex", 0) - selected_frame_index))
            if closest_record and closest_record.get("message"):
                return closest_record
            # Fallback fetch full record if minimal doc
            query = {
                "filename": self.filename,
                "moduleName": self.model_name,
                "projectName": self.project_name,
                "frameIndex": closest_record.get("frameIndex"),
                "email": self.email
            }
            full_records = list(self.db.history_collection.find(query))
            if full_records:
                return full_records[0]
            return closest_record
        except Exception as e:
            logging.error(f"Error finding closest record: {str(e)}")
            return None

    def get_current_time_range(self):
        if not self.time_data:
            return 0, 0
        min_time = min(self.time_data)
        max_time = max(self.time_data)
        time_range = max_time - min_time
        start_time = min_time + (time_range * self.lower_time_percentage / 100.0) if time_range >= 0 else min_time
        end_time = min_time + (time_range * self.upper_time_percentage / 100.0) if time_range >= 0 else max_time
        return start_time, end_time

    def get_current_frame_index_range(self):
        if not self.current_records:
            return 0, 0
        all_frame_indices = [r.get("frameIndex", 0) for r in self.current_records]
        min_frame = min(all_frame_indices)
        max_frame = max(all_frame_indices)
        frame_range = max_frame - min_frame
        start_frame_index = int(min_frame + (frame_range * self.lower_time_percentage / 100.0)) if frame_range >= 0 else min_frame
        end_frame_index = int(min_frame + (frame_range * self.upper_time_percentage / 100.0)) if frame_range >= 0 else max_frame
        return start_frame_index, end_frame_index

    def select_button_click(self):
        try:
            if not self.is_crosshair_locked or not self.locked_crosshair_position:
                mb = self._create_styled_messagebox(
                    title="Selection Required",
                    text=(
                        "Please click on the plot to lock the crosshair at the desired position first,\n"
                        "then click Select."
                    ),
                    icon=QMessageBox.Information,
                    buttons=QMessageBox.Ok,
                    default=QMessageBox.Ok,
                )
                mb.exec_()
                logging.info("Select button clicked but crosshair not locked")
                return

            x, y = self.locked_crosshair_position
            
            # Convert datetime to timestamp for finding closest record
            if hasattr(x, 'timestamp'):
                selected_timestamp = x.timestamp()
            else:
                selected_timestamp = x
                
            self.selected_record = self.find_closest_record_by_timestamp(selected_timestamp)

            if not self.selected_record:
                mb = self._create_styled_messagebox(
                    title="No Record Found",
                    text="No record found for the locked crosshair position.",
                    icon=QMessageBox.Warning,
                    buttons=QMessageBox.Ok,
                    default=QMessageBox.Ok,
                )
                mb.exec_()
                logging.info("No record found for locked crosshair position")
                return

            start_time, end_time = self.get_current_time_range()

            selected_data = {
                "filename": self.filename,
                "model": self.model_name,
                "frameIndex": self.selected_record.get("frameIndex"),
                "timestamp": self.selected_record.get("createdAt"),
                "channelData": self.selected_record.get("message", []),
                "project_name": self.project_name,
                "numberOfChannels": self.selected_record.get("numberOfChannels", 0),
                "tacoChannelCount": self.selected_record.get("tacoChannelCount", 0),
                "samplingRate": self.selected_record.get("samplingRate", 0),
                "samplingSize": self.selected_record.get("samplingSize", 0),
            }

            # Format timestamp for display
            selected_time_str = datetime.datetime.fromtimestamp(selected_timestamp).strftime('%H:%M:%S')
            start_time_str = datetime.datetime.fromtimestamp(start_time).strftime('%H:%M:%S')
            end_time_str = datetime.datetime.fromtimestamp(end_time).strftime('%H:%M:%S')

            confirmation_message = (
                f"Final Confirmation - Range Selection Details:\n\n"
                f"Selected Time: {selected_time_str}\n"
                f"Selected Frame Index: {selected_data['frameIndex']}\n"
                f"Filename: {self.filename}\n"
                f"Model: {self.model_name}\n"
                f"Frequency Value: {y:.2f}\n\n"
                f"Current Range Selection:\n"
                f" Start Time: {start_time_str}\n"
                f" End Time: {end_time_str}\n"
                f" Range: {self.lower_time_percentage:.1f}% to {self.upper_time_percentage:.1f}%\n\n"
                f"Confirm final selection?\n"
                f"The frequency plot will close after confirmation."
            )
            mb = self._create_styled_messagebox(
                title="Final Confirmation - Frame Range Information",
                text=confirmation_message,
                icon=QMessageBox.Question,
                buttons=QMessageBox.Yes | QMessageBox.No,
                default=QMessageBox.No,
            )
            result = mb.exec_()
            if result == QMessageBox.Yes:
                self.time_range_selected.emit(selected_data)
                logging.info(f"Data confirmed for FrameIndex: {selected_data['frameIndex']}, Time Range: {start_time_str} to {end_time_str}")
                mb_done = self._create_styled_messagebox(
                    title="Selection Complete",
                    text=(
                        f"Selection confirmed.\n"
                        f"Frame Index: <b>{selected_data['frameIndex']}</b> selected.\n"
                        f"Time Range: {start_time_str} to {end_time_str}\n\n"
                        f"The frequency plot will now close."
                    ),
                    icon=QMessageBox.Information,
                    buttons=QMessageBox.Ok,
                    default=QMessageBox.Ok,
                )
                mb_done.exec_()
                if self.parent() and hasattr(self.parent(), "close"):
                    self.parent().close()
        except Exception as e:
            logging.error(f"Error in select button click: {str(e)}")
            mb_err = self._create_styled_messagebox(
                title="Error",
                text=f"Error during selection: {str(e)}",
                icon=QMessageBox.Critical,
                buttons=QMessageBox.Ok,
                default=QMessageBox.Ok,
            )
            mb_err.exec_()

    def _create_styled_messagebox(self, title: str, text: str, icon=QMessageBox.Information, buttons=QMessageBox.Ok, default=QMessageBox.Ok) -> QMessageBox:
        # Create the message box
        mb = QMessageBox()
        mb.setWindowTitle(title)
        mb.setText(f"<div style='font-size:14px; color:#333333;'><b>{title}</b></div>")
        mb.setInformativeText(text.replace("\n", "<br/>"))
        mb.setIcon(icon)
        mb.setStandardButtons(buttons)
        
        # Set default button
        if default == QMessageBox.Yes:
            mb.setDefaultButton(QMessageBox.Yes)
        elif default == QMessageBox.No:
            mb.setDefaultButton(QMessageBox.No)
        elif default == QMessageBox.Ok:
            mb.setDefaultButton(QMessageBox.Ok)
        elif default == QMessageBox.Cancel:
            mb.setDefaultButton(QMessageBox.Cancel)
        
        # Apply stylesheet with !important to override any default styles
        mb.setStyleSheet("""
            QMessageBox {
                background-color: #ffffff;
                border: 2px solid #4a90e2;
                border-radius: 8px;
                padding: 12px;
                min-width: 400px;
            }
            QMessageBox QLabel {
                color: #333333;
                font-size: 14px;
                padding: 5px;
            }
            QMessageBox QPushButton {
                min-width: 100px !important;
                padding: 8px 16px !important;
                margin: 5px !important;
                border-radius: 4px !important;
                font-weight: bold !important;
                border: 1px solid #357abd !important;
                background-color: #4a90e2 !important;
                color: white !important;
            }
            QMessageBox QPushButton:hover {
                background-color: #357abd !important;
            }
            QMessageBox QPushButton:pressed {
                background-color: #2c5d9b !important;
            }
            /* Specific button styles */
            QMessageBox QPushButton[text="OK"],
            QMessageBox QPushButton[text="&Yes"],
            QMessageBox QPushButton[text="Yes"],
            QMessageBox QPushButton[text="Confirm"],
            QMessageBox QPushButton[text="&No"],
            QMessageBox QPushButton[text="No"],
            QMessageBox QPushButton[text="Cancel"] {
                min-width: 100px !important;
                padding: 8px 16px !important;
                margin: 5px !important;
                border-radius: 4px !important;
                font-weight: bold !important;
                border: 1px solid #357abd !important;
                background-color: #4a90e2 !important;
                color: white !important;
            }
            QMessageBox QPushButton[text="OK"]:hover,
            QMessageBox QPushButton[text="&Yes"]:hover,
            QMessageBox QPushButton[text="Yes"]:hover,
            QMessageBox QPushButton[text="Confirm"]:hover,
            QMessageBox QPushButton[text="&No"]:hover,
            QMessageBox QPushButton[text="No"]:hover,
            QMessageBox QPushButton[text="Cancel"]:hover {
                background-color: #357abd !important;
            }
            QMessageBox QPushButton[text="OK"]:pressed,
            QMessageBox QPushButton[text="&Yes"]:pressed,
            QMessageBox QPushButton[text="Yes"]:pressed,
            QMessageBox QPushButton[text="Confirm"]:pressed,
            QMessageBox QPushButton[text="&No"]:pressed,
            QMessageBox QPushButton[text="No"]:pressed,
            QMessageBox QPushButton[text="Cancel"]:pressed {
                background-color: #2c5d9b !important;
            }
        """)
        
        # Force style update
        mb.setStyle(mb.style())
        return mb





