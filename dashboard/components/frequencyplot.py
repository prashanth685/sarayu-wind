from PyQt5.QtWidgets import (QWidget, QVBoxLayout, QLabel, QPushButton, QSlider,
                             QHBoxLayout, QMessageBox, QSizePolicy)
from PyQt5.QtCore import Qt, pyqtSignal
import pyqtgraph as pg
import numpy as np
import datetime
import logging
from database import Database


class FrequencyPlot(QWidget):
    time_range_selected = pyqtSignal(dict)

    def __init__(self, parent=None, project_name=None, model_name=None, filename=None,
                 start_time=None, end_time=None, email="user@example.com"):
        super().__init__(parent)
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
        self.time_data = None
        self.frequency_data = None

        self.lower_time_percentage = 0
        self.upper_time_percentage = 100

        self.selected_record = None
        self.is_crosshair_locked = False
        self.locked_crosshair_position = None
        self.selected_point = None
        self.selection_line = None
        self.is_destroying = False

        self.initUI()
        self.initialize_data()

    def parse_time(self, time_str):
        try:
            return datetime.datetime.fromisoformat(time_str.replace('Z', '+00:00'))
        except:
            return None

    def initUI(self):
        self.layout = QVBoxLayout()
        self.layout.setContentsMargins(10, 10, 10, 10)
        self.layout.setSpacing(10)

        # Title
        self.title_label = QLabel(f"Frequency Analysis for {self.filename}")
        self.title_label.setStyleSheet("font-size: 16px; font-weight: bold; color: #333;")
        self.layout.addWidget(self.title_label)

        # Plot Widget
        self.plot_widget = pg.PlotWidget()
        self.plot_widget.setBackground('w')
        self.plot_widget.showGrid(x=True, y=True, alpha=0.3)
        self.plot_widget.setLabel('left', 'Frequency', units='Hz')
        self.plot_widget.setLabel('bottom', 'Time')
        self.plot_widget.setTitle('', size='14pt', bold=True)
        self.plot_widget.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.layout.addWidget(self.plot_widget, stretch=1)

        # Crosshair
        self.vLine = pg.InfiniteLine(angle=90, movable=False, pen=pg.mkPen('#333333', width=1, style=Qt.DotLine))
        self.hLine = pg.InfiniteLine(angle=0, movable=False, pen=pg.mkPen('#333333', width=1, style=Qt.DotLine))
        self.plot_widget.addItem(self.vLine, ignoreBounds=True)
        self.plot_widget.addItem(self.hLine, ignoreBounds=True)
        
        # Center dot at crosshair intersection
        self.center_dot = pg.ScatterPlotItem(size=10, brush=pg.mkBrush('red'), pen=pg.mkPen('darkred', width=2))
        self.plot_widget.addItem(self.center_dot, ignoreBounds=True)
        
        # Selected point indicator
        self.selected_dot = pg.ScatterPlotItem(size=12, brush=pg.mkBrush('green'), pen=pg.mkPen('darkgreen', width=2))
        self.plot_widget.addItem(self.selected_dot, ignoreBounds=True)
        
        # Line from selected point to cursor
        self.selection_line = pg.PlotCurveItem(pen=pg.mkPen('green', width=1, style=Qt.DashLine))
        self.plot_widget.addItem(self.selection_line)

        # Red & Green Movable Selection Lines
        self.start_vertical_line = pg.InfiniteLine(angle=90, movable=True,
            pen=pg.mkPen('r', width=3, style=Qt.SolidLine))
        self.end_vertical_line = pg.InfiniteLine(angle=90, movable=True,
            pen=pg.mkPen('g', width=3, style=Qt.SolidLine))

        self.start_vertical_line.sigPositionChanged.connect(self.on_start_line_moved)
        self.end_vertical_line.sigPositionChanged.connect(self.on_end_line_moved)

        self.plot_widget.addItem(self.start_vertical_line, ignoreBounds=True)
        self.plot_widget.addItem(self.end_vertical_line, ignoreBounds=True)

        # Text labels for bands with border and padding
        self.start_band_label = pg.TextItem(html='<div style="text-align: center; color: white; background-color: rgba(255, 0, 0, 0.7); padding: 2px 8px; border: 1px solid #990000; border-radius: 4px;">Start Band</div>', 
                                          anchor=(0.5, 0), border=None, fill=pg.mkBrush(0, 0, 0, 0))
        self.end_band_label = pg.TextItem(html='<div style="text-align: center; color: white; background-color: rgba(0, 200, 0, 0.7); padding: 2px 8px; border: 1px solid #006600; border-radius: 4px;">End Band</div>', 
                                        anchor=(0.5, 0), border=None, fill=pg.mkBrush(0, 0, 0, 0))
        
        # Add labels to the plot
        self.plot_widget.addItem(self.start_band_label, ignoreBounds=True)
        self.plot_widget.addItem(self.end_band_label, ignoreBounds=True)
        
        # Position the labels at the top of the plot
        self.start_band_label.setZValue(100)  # Ensure labels are on top
        self.end_band_label.setZValue(100)

        # Mouse tracking
        self.proxy = pg.SignalProxy(self.plot_widget.scene().sigMouseMoved, rateLimit=60, slot=self.mouseMoved)
        self.plot_widget.scene().sigMouseClicked.connect(self.mouseClicked)

        # === Slider + Buttons Area ===
        self.slider_widget = QWidget()
        self.slider_layout = QHBoxLayout()
        self.slider_widget.setLayout(self.slider_layout)
        self.slider_widget.setMinimumHeight(60)
        self.slider_widget.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)
        self.slider_layout.setContentsMargins(15, 10, 15, 10)
        self.slider_layout.setSpacing(20)

        # Start Label + Slider
        self.start_label = QLabel("Start: --:--:--")
        self.start_label.setStyleSheet("font-size: 14px; color: #333;")
        self.slider_layout.addWidget(self.start_label)

        self.start_slider = QSlider(Qt.Horizontal)
        self.start_slider.setRange(0, 100)
        self.start_slider.setValue(0)
        self.start_slider.valueChanged.connect(self.on_slider_changed)
        self.slider_layout.addWidget(self.start_slider, stretch=2)

        # End Label + Slider
        self.end_label = QLabel("End: --:--:--")
        self.end_label.setStyleSheet("font-size: 14px; color: #333;")
        self.slider_layout.addWidget(self.end_label)

        self.end_slider = QSlider(Qt.Horizontal)
        self.end_slider.setRange(0, 100)
        self.end_slider.setValue(100)
        self.end_slider.valueChanged.connect(self.on_slider_changed)
        self.slider_layout.addWidget(self.end_slider, stretch=2)

        # === "Drag Range" Button - ENHANCED STYLING ===
        self.range_indicator = QPushButton("Drag Range")
        self.range_indicator.setMinimumSize(120, 40)
        self.range_indicator.setMaximumSize(150, 45)
        self.range_indicator.setStyleSheet("""
            QPushButton {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #5e9cea, stop:1 #4a90e2);
                color: white;
                border: 2px solid #357abd;
                padding: 10px 20px;
                border-radius: 8px;
                font-size: 15px;
                font-weight: bold;
                font-family: 'Segoe UI', Arial, sans-serif;
                text-transform: uppercase;
                letter-spacing: 0.5px;
            }
            QPushButton:hover {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #357abd, stop:1 #2968a8);
                border-color: #2968a8;
            }
            QPushButton:pressed {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #2c5d9b, stop:1 #1e4d7c);
                border-color: #1e4d7c;
            }
        """)
        self.range_indicator.setCursor(Qt.PointingHandCursor)
        self.slider_layout.addWidget(self.range_indicator)

        # === "Select" Button - ENHANCED STYLING ===
        self.select_button = QPushButton("Select")
        self.select_button.setMinimumSize(120, 40)
        self.select_button.setMaximumSize(150, 45)
        self.select_button.setStyleSheet("""
            QPushButton {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #5e9cea, stop:1 #4a90e2);
                color: white;
                border: 2px solid #357abd;
                padding: 10px 20px;
                border-radius: 8px;
                font-size: 15px;
                font-weight: bold;
                font-family: 'Segoe UI', Arial, sans-serif;
                text-transform: uppercase;
                letter-spacing: 0.5px;
            }
            QPushButton:hover {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #357abd, stop:1 #2968a8);
                border-color: #2968a8;
            }
            QPushButton:pressed {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #2c5d9b, stop:1 #1e4d7c);
                border-color: #1e4d7c;
            }
            QPushButton:disabled {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #a0c4ff, stop:1 #8ab4f8);
                color: #e0e0e0;
                border-color: #8ab4f8;
            }
        """)
        self.select_button.setCursor(Qt.PointingHandCursor)
        self.select_button.clicked.connect(self.select_button_click)
        self.slider_layout.addWidget(self.select_button)

        self.layout.addWidget(self.slider_widget)
        self.setLayout(self.layout)

    def initialize_data(self):
        try:
            messages = self.db.get_history_messages(self.project_name, self.model_name, filename=self.filename)
            if not messages:
                return

            self.current_records = sorted(messages, key=lambda x: x.get("frameIndex", 0))
            self.time_data = []
            self.frequency_data = []

            for record in self.current_records:
                ts = self.parse_time(record.get("createdAt"))
                ts_val = ts.timestamp() if ts else record.get("frameIndex", 0)

                message = record.get("message", [])
                num_main = record.get("numberOfChannels", 0)
                taco_cnt = record.get("tacoChannelCount", 0)
                samp_size = record.get("samplingSize", 0)

                if taco_cnt > 0 and samp_size > 0 and len(message) >= num_main * samp_size:
                    tacho_data = message[num_main * samp_size : num_main * samp_size + samp_size]
                    sr = record.get("samplingRate", 1000)
                    dt = 1.0 / sr
                    for i, f in enumerate(tacho_data):
                        self.time_data.append(ts_val + i * dt)
                        self.frequency_data.append(float(f) if f else 0)
                else:
                    freq = record.get("messageFrequency", 0)
                    self.time_data.append(ts_val)
                    self.frequency_data.append(float(freq) if freq else 0)

            self.plot_full_data()
            self.update_selection_lines()

        except Exception as e:
            logging.error(f"Initialization error: {e}")

    def plot_full_data(self):
        self.plot_widget.clear()
        self.plot_widget.addItem(self.vLine, ignoreBounds=True)
        self.plot_widget.addItem(self.hLine, ignoreBounds=True)
        self.plot_widget.addItem(self.center_dot, ignoreBounds=True)
        self.plot_widget.addItem(self.selected_dot, ignoreBounds=True)
        self.plot_widget.addItem(self.selection_line, ignoreBounds=True)
        self.plot_widget.addItem(self.start_vertical_line, ignoreBounds=True)
        self.plot_widget.addItem(self.end_vertical_line, ignoreBounds=True)
        self.plot_widget.addItem(self.start_band_label, ignoreBounds=True)
        self.plot_widget.addItem(self.end_band_label, ignoreBounds=True)

        if self.time_data:
            t_arr = np.array(self.time_data)
            f_arr = np.array(self.frequency_data)
            self.plot_widget.plot(t_arr, f_arr, pen=pg.mkPen('b', width=2), symbol=None)

            axis = self.plot_widget.getAxis('bottom')
            n = min(10, len(t_arr))
            if n > 1:
                idx = np.linspace(0, len(t_arr)-1, n, dtype=int)
                labels = [datetime.datetime.fromtimestamp(t).strftime('%H:%M:%S') for t in t_arr[idx]]
                axis.setTicks([list(zip(t_arr[idx], labels))])
            
            # Set default cursor position to center of plot
            self.set_cursor_to_center()

    def update_selection_lines(self):
        if not self.time_data:
            return

        min_t = min(self.time_data)
        max_t = max(self.time_data)
        span = max(max_t - min_t, 1)

        start_t = min_t + span * (self.lower_time_percentage / 100.0)
        end_t = min_t + span * (self.upper_time_percentage / 100.0)

        self.start_vertical_line.sigPositionChanged.disconnect()
        self.end_vertical_line.sigPositionChanged.disconnect()
        self.start_slider.blockSignals(True)
        self.end_slider.blockSignals(True)

        try:
            self.start_vertical_line.setPos(start_t)
            self.end_vertical_line.setPos(end_t)
            self.start_slider.setValue(int(self.lower_time_percentage))
            self.end_slider.setValue(int(self.upper_time_percentage))
            self.start_label.setText(f"Start: {datetime.datetime.fromtimestamp(start_t):%H:%M:%S}")
            self.end_label.setText(f"End: {datetime.datetime.fromtimestamp(end_t):%H:%M:%S}")

            # Position band labels at the top of the plot with proper vertical alignment
            if self.frequency_data:
                # Get the visible y-range
                y_range = self.plot_widget.viewRange()[1]
                # Position labels slightly above the top of the plot
                label_y = y_range[1] - (y_range[1] - y_range[0]) * 0.05  # 5% from top
                
                if self.start_band_label:
                    self.start_band_label.setPos(start_t, label_y)
                if self.end_band_label:
                    self.end_band_label.setPos(end_t, label_y)
        finally:
            self.start_vertical_line.sigPositionChanged.connect(self.on_start_line_moved)
            self.end_vertical_line.sigPositionChanged.connect(self.on_end_line_moved)
            self.start_slider.blockSignals(False)
            self.end_slider.blockSignals(False)

    def on_slider_changed(self):
        lower = self.start_slider.value()
        upper = self.end_slider.value()

        if lower > upper:
            if self.sender() == self.start_slider:
                lower = upper
                self.start_slider.setValue(lower)
            else:
                upper = lower
                self.end_slider.setValue(upper)

        self.lower_time_percentage = lower
        self.upper_time_percentage = upper
        self.update_selection_lines()

    def on_start_line_moved(self):
        if not self.time_data: return
        pos = self.start_vertical_line.value()
        min_t, max_t = min(self.time_data), max(self.time_data)
        span = max(max_t - min_t, 1)
        pct = max(0, min(100, (pos - min_t) / span * 100))
        self.lower_time_percentage = pct
        if pct > self.upper_time_percentage:
            self.upper_time_percentage = pct
        self.update_selection_lines()

    def on_end_line_moved(self):
        if not self.time_data: return
        pos = self.end_vertical_line.value()
        min_t, max_t = min(self.time_data), max(self.time_data)
        span = max(max_t - min_t, 1)
        pct = max(0, min(100, (pos - min_t) / span * 100))
        self.upper_time_percentage = pct
        if pct < self.lower_time_percentage:
            self.lower_time_percentage = pct
        self.update_selection_lines()

    def mouseMoved(self, evt):
        if not evt: return
        pos = evt[0]
        if self.plot_widget.plotItem.vb.sceneBoundingRect().contains(pos):
            mp = self.plot_widget.plotItem.vb.mapSceneToView(pos)
            
            # Snap cursor to nearest frequency data point
            if self.time_data and self.frequency_data:
                closest_x, closest_y = self.snap_to_nearest_data_point(mp.x(), mp.y())
                self.vLine.setPos(closest_x)
                self.hLine.setPos(closest_y)
                # Update center dot position
                self.center_dot.setData([closest_x], [closest_y])
                
                # Update selection line if we have a selected point
                if self.selected_point is not None:
                    self.selection_line.setData([self.selected_point[0], closest_x], 
                                              [self.selected_point[1], closest_y])
            else:
                self.vLine.setPos(mp.x())
                self.hLine.setPos(mp.y())
                self.center_dot.setData([mp.x()], [mp.y()])

    def mouseClicked(self, evt):
        if not evt: return
        pos = evt.scenePos()
        if self.plot_widget.plotItem.vb.sceneBoundingRect().contains(pos):
            mp = self.plot_widget.plotItem.vb.mapSceneToView(pos)
            
            # Snap to nearest data point when clicking
            if self.time_data and self.frequency_data:
                closest_x, closest_y = self.snap_to_nearest_data_point(mp.x(), mp.y())
                
                # Toggle selection on/off if clicking the same point
                if self.selected_point and abs(self.selected_point[0] - closest_x) < 0.1 and abs(self.selected_point[1] - closest_y) < 0.1:
                    self.selected_point = None
                    self.selected_dot.setData([], [])
                    self.selection_line.setData([], [])
                else:
                    # Select the new point
                    self.selected_point = (closest_x, closest_y)
                    self.selected_dot.setData([closest_x], [closest_y])
                    
                    # Update the selection line to current cursor position
                    self.selection_line.setData([closest_x, closest_x], [closest_y, closest_y])
                
                # Always update crosshair position
                self.locked_crosshair_position = closest_x
                self.vLine.setPos(closest_x)
                self.hLine.setPos(closest_y)
                self.center_dot.setData([closest_x], [closest_y])
            else:
                self.locked_crosshair_position = mp.x()
                self.vLine.setPos(mp.x())
                self.hLine.setPos(mp.y())
                self.center_dot.setData([mp.x()], [mp.y()])

    def select_button_click(self):
        if self.selected_point is None:
            self._show_messagebox("Selection Required",
                                  "Please click on a data point to select it first,\nthen click Select.",
                                  QMessageBox.Information)
            return

        selected_ts = self.selected_point[0] if self.selected_point else self.locked_crosshair_position
        idx = np.argmin(np.abs(np.array(self.time_data) - selected_ts))
        
        # Find the record with timestamp closest to the selected position
        # This ensures we get the correct frame index based on the user's selection
        closest_record = None
        min_diff = float('inf')
        
        for record in self.current_records:
            record_ts = self.parse_time(record.get("createdAt"))
            if record_ts:
                record_ts_val = record_ts.timestamp()
            else:
                record_ts_val = record.get("frameIndex", 0)
            
            diff = abs(record_ts_val - selected_ts)
            if diff < min_diff:
                min_diff = diff
                closest_record = record
        
        record = closest_record if closest_record else self.current_records[min(idx, len(self.current_records)-1)]

        start_t = min(self.time_data) + (max(self.time_data) - min(self.time_data)) * (self.lower_time_percentage / 100)
        end_t = min(self.time_data) + (max(self.time_data) - min(self.time_data)) * (self.upper_time_percentage / 100)

        selected_data = {
            "lower_pct": self.lower_time_percentage,
            "upper_pct": self.upper_time_percentage,
            "filename": self.filename,
            "model": self.model_name,
            "frameIndex": record.get("frameIndex"),
            "timestamp": record.get("createdAt"),
            "project_name": self.project_name,
            "message": record.get("message", []),
            "channelData": record.get("message", []),
            "numberOfChannels": record.get("numberOfChannels", 0),
            "tacoChannelCount": record.get("tacoChannelCount", 0),
            "samplingSize": record.get("samplingSize", 0),
            "samplingRate": record.get("samplingRate", 1000),
            "samples_per_channel": record.get("samplingSize", 0),
            "sample_rate": record.get("samplingRate", 1000),
        }

        msg = (f"<b>Final Confirmation</b><br><br>"
               f"Selected Time: {datetime.datetime.fromtimestamp(selected_ts):%H:%M:%S}<br>"
               f"Frame Index: {selected_data['frameIndex']}<br><br>"
               f"Range Start: {datetime.datetime.fromtimestamp(start_t):%H:%M:%S}<br>"
               f"Range End: {datetime.datetime.fromtimestamp(end_t):%H:%M:%S}<br><br>"
               f"Confirm selection?")

        if self._show_messagebox("Confirm Selection", msg, QMessageBox.Question,
                                 QMessageBox.Yes | QMessageBox.No, QMessageBox.Yes) == QMessageBox.Yes:
            self.time_range_selected.emit(selected_data)
            if self.parent():
                self.parent().close()

    def _show_messagebox(self, title, text, icon=QMessageBox.Information, buttons=QMessageBox.Ok, default=QMessageBox.Ok):
        mb = QMessageBox(self)
        mb.setWindowTitle(title)
        mb.setText(f"<div style='font-size:14px; color:#333333;'><b>{title}</b></div>")
        mb.setInformativeText(text)
        mb.setIcon(icon)
        mb.setStandardButtons(buttons)
        mb.setDefaultButton(default)

        mb.setStyleSheet("""
            QMessageBox {
                background-color: #ffffff;
                border: 2px solid #4a90e2;
                border-radius: 8px;
                min-width: 420px;
            }
            QMessageBox QLabel {
                color: #333333;
                font-size: 14px;
            }
            QPushButton {
                min-width: 100px;
                padding: 10px 20px;
                margin: 8px;
                border-radius: 6px;
                font-weight: bold;
                font-size: 14px;
                background-color: #4a90e2;
                color: white;
                border: 2px solid #357abd;
            }
            QPushButton:hover {
                background-color: #357abd;
                border-color: #2968a8;
            }
            QPushButton:pressed {
                background-color: #2c5d9b;
                border-color: #1e4d7c;
            }
        """)
        
        # Apply direct styling to buttons after they are created
        for button in mb.findChildren(QPushButton):
            button.setStyleSheet("""
                QPushButton {
                    min-width: 100px;
                    padding: 10px 20px;
                    margin: 8px;
                    border-radius: 6px;
                    font-weight: bold;
                    font-size: 14px;
                    background-color: #4a90e2;
                    color: white;
                    border: 2px solid #357abd;
                }
                QPushButton:hover {
                    background-color: #357abd;
                    border-color: #2968a8;
                }
                QPushButton:pressed {
                    background-color: #2c5d9b;
                    border-color: #1e4d7c;
                }
            """)
        
        return mb.exec_()

    def closeEvent(self, event):
        self.is_destroying = True
        try:
            self.start_vertical_line.sigPositionChanged.disconnect()
            self.end_vertical_line.sigPositionChanged.disconnect()
        except:
            pass
        super().closeEvent(event)

    def load_selected_frame(self, payload: dict):
        """Apply a saved selection (cursor + range) when opening an existing file."""
        try:
            if not payload:
                return
            # Apply cursor lock
            frame_idx = payload.get("frameIndex")
            ts_val = None
            if frame_idx is not None:
                # Find record with that frameIndex
                rec = next((r for r in self.current_records if r.get("frameIndex") == frame_idx), None)
                if rec:
                    rec_ts = self.parse_time(rec.get("createdAt"))
                    ts_val = rec_ts.timestamp() if rec_ts else rec.get("frameIndex", None)
            if ts_val is None and payload.get("timestamp"):
                ts_parsed = self.parse_time(payload.get("timestamp"))
                if ts_parsed:
                    ts_val = ts_parsed.timestamp()
            if ts_val is not None:
                # Snap to nearest data point for cursor lock
                if self.time_data and self.frequency_data:
                    closest_x, closest_y = self.snap_to_nearest_data_point(ts_val, 0)
                    # Update selected point visualization
                    self.selected_point = (closest_x, closest_y)
                    self.selected_dot.setData([closest_x], [closest_y])
                    # Update cursor position
                    self.locked_crosshair_position = closest_x
                    self.is_crosshair_locked = True
                    self.vLine.setPos(closest_x)
                    self.hLine.setPos(closest_y)
                    self.center_dot.setData([closest_x], [closest_y])
            # Apply range if provided
            lower_pct = payload.get("lower_pct")
            upper_pct = payload.get("upper_pct")
            if lower_pct is not None and upper_pct is not None:
                self.lower_time_percentage = float(lower_pct)
                self.upper_time_percentage = float(upper_pct)
                self.update_selection_lines()
        except Exception as e:
            logging.error(f"FrequencyPlot: Error loading selected frame: {e}")

    def snap_to_nearest_data_point(self, mouse_x, mouse_y):
        """Snap cursor position to the nearest frequency data point"""
        if not self.time_data or not self.frequency_data:
            return mouse_x, mouse_y
        
        time_array = np.array(self.time_data)
        freq_array = np.array(self.frequency_data)
        
        # Find the nearest time index
        time_idx = np.argmin(np.abs(time_array - mouse_x))
        
        # Return the actual data point coordinates
        return time_array[time_idx], freq_array[time_idx]
    
    def set_cursor_to_center(self):
        """Set cursor position to center of the plot"""
        if not self.time_data or not self.frequency_data:
            return
        
        min_time = min(self.time_data)
        max_time = max(self.time_data)
        min_freq = min(self.frequency_data)
        max_freq = max(self.frequency_data)
        
        center_time = (min_time + max_time) / 2
        center_freq = (min_freq + max_freq) / 2
        
        # Snap to nearest data point near center
        closest_x, closest_y = self.snap_to_nearest_data_point(center_time, center_freq)
        
        self.vLine.setPos(closest_x)
        self.hLine.setPos(closest_y)
        self.center_dot.setData([closest_x], [closest_y])