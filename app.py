import sys
import logging
import os
from PyQt5.QtWidgets import QApplication
from PyQt5.QtCore import Qt, QCoreApplication
from PyQt5.QtGui import QIcon
import pyqtgraph as pg
from auth import AuthWindow

if __name__ == '__main__':
    # High-DPI and rendering optimizations
    try:
        QCoreApplication.setAttribute(Qt.AA_EnableHighDpiScaling, True)
        QCoreApplication.setAttribute(Qt.AA_UseHighDpiPixmaps, True)
    except Exception:
        pass

    # Configure pyqtgraph for better performance
    try:
        pg.setConfigOptions(
            useOpenGL=True,           # GPU acceleration where available
            antialias=False,          # disable antialiasing for speed
            downsample=True,          # enable automatic downsampling
            foreground='w',
            background=None
        )
    except Exception:
        pass

    # Reduce logging noise globally (many modules log at DEBUG)
    logging.getLogger().setLevel(logging.WARNING)

    # Let Qt auto-scale per-monitor DPI
    try:
        os.environ.setdefault("QT_AUTO_SCREEN_SCALE_FACTOR", "1")
    except Exception:
        pass

    app = QApplication(sys.argv)
    # Set application icon (affects taskbar and windows)
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        candidates = [
            os.path.join(base_dir, 'logo.ico'),
            os.path.join(base_dir, 'logo.png'),
            os.path.join(base_dir, 'icons', 'logo.png'),
        ]
        icon_path = next((p for p in candidates if os.path.exists(p)), None)
        if icon_path:
            app.setWindowIcon(QIcon(icon_path))
    except Exception:
        pass
    auth_window = AuthWindow()
    auth_window.show()
    sys.exit(app.exec_())