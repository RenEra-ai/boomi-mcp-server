"""
Coordinate Calculator for Boomi Process Shapes.

Handles automatic positioning and spacing of shapes in process diagrams.
Based on analysis of real Boomi process XML.
"""

from typing import List, Tuple


class CoordinateCalculator:
    """Calculate shape positions and dragpoint coordinates automatically."""

    # Default constants extracted from real processes
    DEFAULT_SPACING = 192.0  # Horizontal spacing between shapes
    DEFAULT_Y = 48.0  # Default Y position for linear flow
    DEFAULT_START_X = 48.0  # Starting X position

    # Dragpoint offsets (relative to shape position)
    DRAGPOINT_OFFSET_X = 176.0  # Dragpoint X relative to shape X
    DRAGPOINT_OFFSET_Y = 10.0   # Dragpoint Y relative to shape Y

    def __init__(self):
        """Initialize coordinate calculator."""
        pass

    def calculate_linear_layout(
        self,
        num_shapes: int,
        start_x: float = None,
        y_position: float = None,
        spacing: float = None
    ) -> List[Tuple[float, float]]:
        """
        Calculate coordinates for linear (horizontal) flow.

        Args:
            num_shapes: Number of shapes to position
            start_x: Starting X coordinate (default: 48.0)
            y_position: Y coordinate for all shapes (default: 48.0)
            spacing: Horizontal spacing between shapes (default: 192.0)

        Returns:
            List of (x, y) coordinate tuples

        Example:
            >>> calc = CoordinateCalculator()
            >>> coords = calc.calculate_linear_layout(num_shapes=5)
            >>> coords
            [(48.0, 48.0), (240.0, 48.0), (432.0, 48.0), (656.0, 48.0), (848.0, 48.0)]
        """
        x = start_x if start_x is not None else self.DEFAULT_START_X
        y = y_position if y_position is not None else self.DEFAULT_Y
        spacing = spacing if spacing is not None else self.DEFAULT_SPACING

        coordinates = []
        current_x = x

        for i in range(num_shapes):
            coordinates.append((current_x, y))
            current_x += spacing

        return coordinates

    def calculate_dragpoint(
        self,
        shape_x: float,
        shape_y: float,
        offset_x: float = None,
        offset_y: float = None
    ) -> Tuple[float, float]:
        """
        Calculate dragpoint coordinates from shape position.

        Args:
            shape_x: Shape X coordinate
            shape_y: Shape Y coordinate
            offset_x: X offset from shape (default: 176.0)
            offset_y: Y offset from shape (default: 10.0)

        Returns:
            (drag_x, drag_y) tuple

        Example:
            >>> calc = CoordinateCalculator()
            >>> drag_x, drag_y = calc.calculate_dragpoint(48.0, 46.0)
            >>> (drag_x, drag_y)
            (224.0, 56.0)
        """
        offset_x = offset_x if offset_x is not None else self.DRAGPOINT_OFFSET_X
        offset_y = offset_y if offset_y is not None else self.DRAGPOINT_OFFSET_Y

        drag_x = shape_x + offset_x
        drag_y = shape_y + offset_y

        return (drag_x, drag_y)
