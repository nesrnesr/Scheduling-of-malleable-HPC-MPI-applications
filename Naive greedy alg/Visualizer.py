from dataclasses import dataclass
import cairocffi as cairo

@dataclass
class Vector2i():
    x: int = 0
    y: int = 0

class Visualizer():
    
