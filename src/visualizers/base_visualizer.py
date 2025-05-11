"""
Base Visualizer class with common functionality for all data visualizers.
"""
import matplotlib.pyplot as plt
from pathlib import Path

import config
from src.utils.file_utils import ensure_directory


class BaseVisualizer:
    """
    Base class for all data visualizers with shared functionality.
    """
    
    def __init__(self):
        """Initialize the base visualizer."""
        self.processed_dir = config.PROCESSED_DATA_DIR
        self.analysis_dir = config.ANALYSIS_DIR
        self.reports_dir = config.REPORTS_DIR
        self.figure_sizes = config.FIGURE_SIZES
        self.dpi = config.DEFAULT_DPI
        self.save_figures = config.SAVE_FIGURES
    
    def get_processed_file_path(self, race_name, session_name, topic_name, file_name):
        """
        Get the path to a processed data file.
        
        Args:
            race_name: Name of the race
            session_name: Name of the session
            topic_name: Name of the data topic
            file_name: Name of the processed file
            
        Returns:
            Path: Path to the processed file
        """
        return self.processed_dir / race_name / session_name / topic_name / file_name
    
    def save_figure(self, figure, race_name, session_name, viz_type, fig_name):
        """
        Save a matplotlib figure to file.
        
        Args:
            figure: The matplotlib figure to save
            race_name: Name of the race
            session_name: Name of the session
            viz_type: Type of visualization
            fig_name: Name for the figure file
            
        Returns:
            Path: Path to the saved figure
        """
        output_dir = self.analysis_dir / race_name / session_name / viz_type
        ensure_directory(output_dir)
        
        fig_path = output_dir / f"{fig_name}.png"
        figure.savefig(fig_path, dpi=self.dpi, bbox_inches='tight')
        plt.close(figure)
        
        print(f"Figure saved to {fig_path}")
        return fig_path
    
    def create_visualizations(self, race_name, session_name, driver_numbers=None):
        """
        Create visualizations for a specific race and session.
        This method should be implemented by subclasses.
        
        Args:
            race_name: Name of the race
            session_name: Name of the session
            driver_numbers: Optional list of driver numbers to visualize
            
        Returns:
            dict: Visualization results
        """
        raise NotImplementedError("Subclasses must implement this method")