#!/usr/bin/env python3
"""
Test script to verify frame index only appears when selected in current session
"""

import sys
import os

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from PyQt5.QtWidgets import QApplication, QWidget, QLabel, QMdiSubWindow, QMdiArea
from PyQt5.QtCore import Qt

class MinimalMainSection(QWidget):
    """Minimal version of MainSection to test add_subwindow logic"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.mdi_area = QMdiArea()
        self.parent = parent
    
    def add_subwindow(self, widget, feature_name, project_name=None, channel_name=None, model_name=None, frame_index=None):
        """Simplified version of add_subwindow for testing"""
        try:
            subwindow = QMdiSubWindow()
            subwindow.setWidget(widget)
            subwindow.setOption(QMdiSubWindow.RubberBandMove, False)
            subwindow.setWindowFlags(subwindow.windowFlags() & ~Qt.WindowMinimizeButtonHint)
            # Ensure project_name is always resolved: fall back to parent's current project if not provided
            resolved_project = project_name or getattr(self.parent, 'current_project', None) or ''
            title = f"{resolved_project} - {feature_name}".strip(" - ")
            # Add frame index to title if available
            if frame_index is not None:
                title += f" - Frame {frame_index}"
            subwindow.setWindowTitle(title)
            self.mdi_area.addSubWindow(subwindow)
            subwindow.showNormal()
            return subwindow
        except Exception as e:
            print(f"Failed to add subwindow for {feature_name}: {str(e)}")
            return None

class MockDashboard(QWidget):
    """Mock dashboard to test frame selection behavior"""
    
    def __init__(self):
        super().__init__()
        self.current_project = "TestProject"
        self.last_selection_payload_by_model = {}
        self.current_session_frame_selections = {}
    
    def simulate_frame_selection(self, model_name, frame_index):
        """Simulate selecting a frame in the current session"""
        self.last_selection_payload_by_model[model_name] = {"frameIndex": frame_index}
        self.current_session_frame_selections[model_name] = frame_index
    
    def simulate_stale_selection(self, model_name, frame_index):
        """Simulate having a stale selection (not in current session)"""
        self.last_selection_payload_by_model[model_name] = {"frameIndex": frame_index}
        # Don't add to current_session_frame_selections
    
    def clear_session(self):
        """Clear current session selections"""
        self.current_session_frame_selections = {}
    
    def get_frame_index_for_window(self, model_name):
        """Get frame index to display in window title"""
        if model_name in self.current_session_frame_selections:
            return self.current_session_frame_selections.get(model_name)
        return None

def test_frame_index_session_behavior():
    """Test that frame index only appears when selected in current session"""
    app = QApplication(sys.argv)
    
    # Create mock dashboard
    dashboard = MockDashboard()
    
    # Create MinimalMainSection
    main_section = MinimalMainSection(dashboard)
    
    # Create a test widget
    test_widget = QLabel("Test Content")
    
    # Test 1: No selection at all
    frame_index = dashboard.get_frame_index_for_window("TestModel")
    subwindow1 = main_section.add_subwindow(
        test_widget,
        "Test Feature",
        project_name="TestProject",
        model_name="TestModel",
        channel_name="TestChannel",
        frame_index=frame_index
    )
    
    title1 = subwindow1.windowTitle()
    print(f"Test 1 - No selection: {title1}")
    assert "Frame" not in title1, "Frame index should not appear when no selection"
    
    # Test 2: Stale selection (not in current session)
    dashboard.simulate_stale_selection("TestModel", 42)
    frame_index = dashboard.get_frame_index_for_window("TestModel")
    subwindow2 = main_section.add_subwindow(
        test_widget,
        "Test Feature",
        project_name="TestProject",
        model_name="TestModel",
        channel_name="TestChannel",
        frame_index=frame_index
    )
    
    title2 = subwindow2.windowTitle()
    print(f"Test 2 - Stale selection: {title2}")
    assert "Frame" not in title2, "Frame index should not appear for stale selections"
    
    # Test 3: Current session selection
    dashboard.simulate_frame_selection("TestModel", 42)
    frame_index = dashboard.get_frame_index_for_window("TestModel")
    subwindow3 = main_section.add_subwindow(
        test_widget,
        "Test Feature",
        project_name="TestProject",
        model_name="TestModel",
        channel_name="TestChannel",
        frame_index=frame_index
    )
    
    title3 = subwindow3.windowTitle()
    print(f"Test 3 - Current session selection: {title3}")
    assert "Frame 42" in title3, "Frame index should appear for current session selections"
    
    # Test 4: After clearing session
    dashboard.clear_session()
    frame_index = dashboard.get_frame_index_for_window("TestModel")
    subwindow4 = main_section.add_subwindow(
        test_widget,
        "Test Feature",
        project_name="TestProject",
        model_name="TestModel",
        channel_name="TestChannel",
        frame_index=frame_index
    )
    
    title4 = subwindow4.windowTitle()
    print(f"Test 4 - After clearing session: {title4}")
    assert "Frame" not in title4, "Frame index should not appear after clearing session"
    
    print("\nAll tests passed!")
    app.quit()

if __name__ == "__main__":
    test_frame_index_session_behavior()
