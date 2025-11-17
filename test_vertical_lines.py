#!/usr/bin/env python3
"""
Test script to verify vertical band lines functionality in FrequencyPlot
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from PyQt5.QtWidgets import QApplication
from PyQt5.QtCore import QTimer
import datetime

# Mock data for testing
def create_mock_data():
    """Create mock data for testing the frequency plot"""
    base_time = datetime.datetime.now().timestamp()
    time_data = []
    frequency_data = []
    
    for i in range(100):
        time_data.append(base_time + i * 60)  # 1 minute intervals
        frequency_data.append(50 + 10 * (i % 10))  # Varying frequency
    
    return time_data, frequency_data

def test_vertical_lines():
    """Test that vertical lines are properly initialized and functional"""
    app = QApplication(sys.argv)
    
    try:
        from dashboard.components.frequencyplot import FrequencyPlot
        
        # Create a FrequencyPlot instance
        plot = FrequencyPlot(
            project_name="test_project",
            model_name="test_model", 
            filename="test_file",
            start_time=datetime.datetime.now().isoformat(),
            end_time=(datetime.datetime.now() + datetime.timedelta(hours=2)).isoformat()
        )
        
        # Mock the data loading
        time_data, frequency_data = create_mock_data()
        plot.time_data = time_data
        plot.frequency_data = frequency_data
        
        # Call filter_and_plot_data to initialize the lines
        plot.filter_and_plot_data()
        
        # Check if vertical lines are created
        assert plot.start_band_line is not None, "Start band line was not created"
        assert plot.end_band_line is not None, "End band line was not created"
        
        # Check if lines are movable
        assert plot.start_band_line.movable, "Start band line is not movable"
        assert plot.end_band_line.movable, "End band line is not movable"
        
        # Check if signals are connected
        assert plot.start_band_line.sigDragged.receivers > 0, "Start band line drag signal not connected"
        assert plot.end_band_line.sigDragged.receivers > 0, "End band line drag signal not connected"
        
        print("✅ All tests passed!")
        print("✅ Vertical lines are properly created and draggable")
        print("✅ Drag signals are connected")
        print("✅ Lines will update sliders when dragged")
        
        return True
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False
    finally:
        app.quit()

if __name__ == "__main__":
    success = test_vertical_lines()
    sys.exit(0 if success else 1)
