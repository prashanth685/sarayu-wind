#!/usr/bin/env python3
"""
Test script to verify frame index display in window titles
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

def test_frame_index_display():
    """Test that frame index is correctly included in window title"""
    app = QApplication(sys.argv)
    
    # Create a mock parent widget
    parent = QWidget()
    parent.current_project = "TestProject"
    
    # Create MinimalMainSection
    main_section = MinimalMainSection(parent)
    
    # Create a test widget
    test_widget = QLabel("Test Content")
    
    # Test 1: Without frame index
    subwindow1 = main_section.add_subwindow(
        test_widget,
        "Test Feature",
        project_name="TestProject",
        model_name="TestModel",
        channel_name="TestChannel"
    )
    
    title1 = subwindow1.windowTitle()
    print(f"Title without frame index: {title1}")
    assert "Frame" not in title1, "Frame index should not appear in title when not provided"
    assert "TestProject - Test Feature" == title1, f"Expected 'TestProject - Test Feature', got '{title1}'"
    
    # Test 2: With frame index
    subwindow2 = main_section.add_subwindow(
        test_widget,
        "Test Feature",
        project_name="TestProject",
        model_name="TestModel",
        channel_name="TestChannel",
        frame_index=42
    )
    
    title2 = subwindow2.windowTitle()
    print(f"Title with frame index: {title2}")
    assert "Frame 42" in title2, "Frame index should appear in title when provided"
    assert "TestProject - Test Feature - Frame 42" == title2, f"Expected 'TestProject - Test Feature - Frame 42', got '{title2}'"
    
    # Test 3: With None frame index
    subwindow3 = main_section.add_subwindow(
        test_widget,
        "Test Feature",
        project_name="TestProject",
        model_name="TestModel",
        channel_name="TestChannel",
        frame_index=None
    )
    
    title3 = subwindow3.windowTitle()
    print(f"Title with None frame index: {title3}")
    assert "Frame" not in title3, "Frame index should not appear in title when None"
    assert "TestProject - Test Feature" == title3, f"Expected 'TestProject - Test Feature', got '{title3}'"
    
    print("\nAll tests passed!")
    app.quit()

if __name__ == "__main__":
    test_frame_index_display()
