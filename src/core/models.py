from pydantic import BaseModel, Field
from typing import List, Optional

class Paragraph(BaseModel):
    name: str
    code: str
    summary: Optional[str] = None
    calls: List[str] = Field(default_factory=list)

class Section(BaseModel):
    name: str
    code: str
    summary: Optional[str] = None
    paragraphs: List[Paragraph] = Field(default_factory=list)

class Division(BaseModel):
    name: str
    code: str
    summary: Optional[str] = None
    sections: List[Section] = Field(default_factory=list)
    call_graph_mermaid: Optional[str] = None

class Program(BaseModel):
    filename: str
    program_name: Optional[str] = None
    content: str
    divisions: List[Division] = Field(default_factory=list)