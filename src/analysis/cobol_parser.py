import re
from typing import List, Optional
from src.core.models import Program, Division, Section, Paragraph

# --- REGEX PATTERNS ---

# (Existing patterns)
RE_DIVISION = re.compile(
    r"^[ \t]*((IDENTIFICATION|ENVIRONMENT|DATA)|(PROCEDURE[ \t\.]+(USING|CHAINING)?[ \t]*[A-Z0-9-]+(\([A-Z0-9-]+\))?)|(PROCEDURE))[ \t]*DIVISION[ \t]*\.",
    re.IGNORECASE | re.MULTILINE
)
RE_SECTION = re.compile(
    r"^[ \t]*([A-Z0-9-]+)[ \t]+SECTION[ \t]*\.",
    re.IGNORECASE | re.MULTILINE
)
RE_PARAGRAPH = re.compile(
    r"^[ \t]*([A-Z0-9][A-Z0-9-]*)[ \t]*\.(?![ \t]*\.)", 
    re.IGNORECASE | re.MULTILINE
)
RE_PROGRAM_ID = re.compile(
    r"^[ \t]*PROGRAM-ID\.[ \t]*([A-Z0-9-]+)[ \t]*\.",
    re.IGNORECASE | re.MULTILINE
)

# NEW: Regex to find PERFORM statements (basic version)
# Looks for "PERFORM" followed by a valid paragraph/section name
RE_PERFORM = re.compile(
    r"PERFORM[ \t]+([A-Z0-9][A-Z0-9-]*)",
    re.IGNORECASE
)

# --- HELPER FUNCTIONS ---

def clean_code(code_lines: List[str]) -> str:
    """Removes standard COBOL comment lines and line numbers."""
    cleaned_lines = []
    for line in code_lines:
        if len(line) > 7:
            if line[6] == '*': continue
            if line[6].upper() == 'D': continue
        
        if len(line) >= 80: 
             cleaned_lines.append(line[6:72].rstrip())
        elif len(line) > 6:
            cleaned_lines.append(line[6:].rstrip())
        else:
            cleaned_lines.append(line.rstrip())
            
    return "\n".join(cleaned_lines).strip()

def split_by_regex(text: str, regex: re.Pattern) -> List[dict]:
    """Splits text by a regex, returning a list of {'name':, 'code':} dicts."""
    matches = list(regex.finditer(text))
    parts = []
    
    if not matches:
        return [{'name': 'DEFAULT', 'code': text}]

    start_pos = 0
    if matches[0].start() > 0:
        parts.append({
            'name': 'HEADER', 
            'code': text[0:matches[0].start()]
        })

    for i, match in enumerate(matches):
        name = match.group(1).strip().upper()
        start_pos = match.end()
        end_pos = len(text)
        if i + 1 < len(matches):
            end_pos = matches[i+1].start()
            
        code = text[start_pos:end_pos].strip()
        parts.append({'name': name, 'code': code})
        
    return parts

def generate_mermaid_graph(division: Division) -> str:
    """Generates a MermaidJS graph from the parsed procedure division."""
    graph_lines = ["graph TD;"]
    nodes = set()
    
    for section in division.sections:
        for paragraph in section.paragraphs:
            # Use paragraph name as a unique ID
            caller_id = paragraph.name
            nodes.add(caller_id)
            
            for target_call in paragraph.calls:
                target_id = target_call
                nodes.add(target_id)
                # Add the edge
                graph_lines.append(f"    {caller_id} --> {target_id};")

    # Add node definitions (in case some are only called and never defined)
    for node in nodes:
        graph_lines.append(f"    {node}([{node}]);")
                
    return "\n".join(graph_lines)

# --- MAIN PARSER ---

def parse_program(filename: str, content: str) -> Program:
    """Parses raw COBOL content into a structured Program object."""
    
    content = content.replace('\r\n', '\n')
    
    program_name = "UNKNOWN"
    program_id_match = RE_PROGRAM_ID.search(content)
    if program_id_match:
        program_name = program_id_match.group(1).upper()
    
    program = Program(filename=filename, program_name=program_name, content=content)

    # 1. Split by DIVISION
    division_parts = split_by_regex(content, RE_DIVISION)
    
    for div_part in division_parts:
        if div_part['name'] == 'HEADER': continue
            
        div_name = div_part['name']
        if div_name.startswith("PROCEDURE"):
            div_name = "PROCEDURE"
            
        division = Division(name=div_name, code=clean_code(div_part['code'].split('\n')))
        program.divisions.append(division)

        # 2. Split by SECTION
        section_parts = split_by_regex(division.code, RE_SECTION)
        
        for sec_part in section_parts:
            section = Section(name=sec_part['name'], code=sec_part['code'])
            division.sections.append(section)

            # 3. Split PROCEDURE DIVISION sections by PARAGRAPH
            if division.name == "PROCEDURE":
                paragraph_parts = split_by_regex(section.code, RE_PARAGRAPH)
                
                if paragraph_parts[0]['name'] == 'DEFAULT' or paragraph_parts[0]['name'] == 'HEADER':
                    if paragraph_parts[0]['code'].strip():
                         # Find calls in the "header" code
                         header_code = paragraph_parts[0]['code']
                         header_calls = [call.upper() for call in RE_PERFORM.findall(header_code)]
                         section.paragraphs.append(Paragraph(
                             name=f"{section.name}-HEADER", 
                             code=header_code,
                             calls=header_calls
                         ))
                    if len(paragraph_parts) > 1:
                        paragraph_parts = paragraph_parts[1:]
                    
                
                for para_part in paragraph_parts:
                    if para_part['name'] in ('DEFAULT', 'HEADER') and len(paragraph_parts) > 1:
                        continue
                    
                    # NEW: Find all PERFORM calls within this paragraph's code
                    para_code = para_part['code']
                    para_calls = [call.upper() for call in RE_PERFORM.findall(para_code)]
                    
                    paragraph = Paragraph(
                        name=para_part['name'], 
                        code=para_code,
                        calls=para_calls # Store the calls
                    )
                    section.paragraphs.append(paragraph)

        # 4. NEW: Generate Mermaid graph *after* all sections/paras are parsed
        if division.name == "PROCEDURE":
            division.call_graph_mermaid = generate_mermaid_graph(division)

    return program