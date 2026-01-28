#!/usr/bin/env python3
"""
Verify server.py and server_local.py are in sync.

This script checks that both server files have matching:
- Function parameters for all MCP tools
- CREATE section fields (request_data assignments)
- UPDATE section fields (updates assignments)

Exit codes:
- 0: All checks passed
- 1: Files are out of sync
"""

import re
import sys
from pathlib import Path


def extract_function_params(content: str, func_name: str) -> set:
    """Extract parameter names from a function definition."""
    # Find the function definition
    pattern = rf'def {func_name}\s*\(([\s\S]*?)\):'
    match = re.search(pattern, content)
    if not match:
        return set()

    params_block = match.group(1)
    # Extract parameter names (before : or =)
    params = set()
    for line in params_block.split('\n'):
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        # Match parameter name at start of line
        param_match = re.match(r'^(\w+)\s*:', line)
        if param_match:
            params.add(param_match.group(1))

    return params


def extract_section_fields(content: str, func_name: str, section: str) -> set:
    """Extract field names from CREATE or UPDATE section of a function.

    Args:
        content: File content
        func_name: Function name to search within
        section: "create" or "update"

    Returns:
        Set of field names
    """
    # Find the function first
    func_pattern = rf'def {func_name}\s*\('
    func_match = re.search(func_pattern, content)
    if not func_match:
        return set()

    func_start = func_match.start()

    # Find the next function definition to limit our search
    next_func = re.search(r'\n    def \w+\(', content[func_start + 100:])
    if next_func:
        func_end = func_start + 100 + next_func.start()
    else:
        func_end = len(content)

    func_content = content[func_start:func_end]

    # Find the section (CREATE or UPDATE)
    if section == "create":
        section_pattern = r'elif action == "create":([\s\S]*?)(?:elif action == |$)'
        dict_name = "request_data"
    else:
        section_pattern = r'elif action == "update":([\s\S]*?)(?:elif action == |$)'
        dict_name = "updates"

    section_match = re.search(section_pattern, func_content)
    if not section_match:
        return set()

    section_content = section_match.group(1)

    # Extract field names from dict assignments
    field_pattern = rf'{dict_name}\["(\w+)"\]'
    fields = set(re.findall(field_pattern, section_content))

    return fields


def compare_and_report(name: str, main_set: set, dev_set: set) -> bool:
    """Compare two sets and report differences.

    Returns True if they match, False otherwise.
    """
    main_only = main_set - dev_set
    dev_only = dev_set - main_set

    if main_only or dev_only:
        if main_only:
            print(f"    Missing in dev: {', '.join(sorted(main_only))}")
        if dev_only:
            print(f"    Missing in main: {', '.join(sorted(dev_only))}")
        return False

    return True


def check_function(main_content: str, dev_content: str, func_name: str,
                   check_create: bool = True, check_update: bool = True) -> bool:
    """Check a function for sync between main and dev.

    Returns True if all checks pass.
    """
    print(f"\nChecking {func_name}...")
    all_passed = True

    # Check function parameters
    main_params = extract_function_params(main_content, func_name)
    dev_params = extract_function_params(dev_content, func_name)

    if main_params == dev_params:
        print(f"  Function params: {len(main_params)} = {len(dev_params)} ✅")
    else:
        print(f"  Function params: {len(main_params)} vs {len(dev_params)} ❌")
        all_passed = compare_and_report("params", main_params, dev_params) and all_passed

    # Check CREATE section
    if check_create:
        main_create = extract_section_fields(main_content, func_name, "create")
        dev_create = extract_section_fields(dev_content, func_name, "create")

        if main_create == dev_create:
            print(f"  CREATE fields:   {len(main_create)} = {len(dev_create)} ✅")
        else:
            print(f"  CREATE fields:   {len(main_create)} vs {len(dev_create)} ❌")
            all_passed = compare_and_report("CREATE", main_create, dev_create) and all_passed

    # Check UPDATE section
    if check_update:
        main_update = extract_section_fields(main_content, func_name, "update")
        dev_update = extract_section_fields(dev_content, func_name, "update")

        if main_update == dev_update:
            print(f"  UPDATE fields:   {len(main_update)} = {len(dev_update)} ✅")
        else:
            print(f"  UPDATE fields:   {len(main_update)} vs {len(dev_update)} ❌")
            all_passed = compare_and_report("UPDATE", main_update, dev_update) and all_passed

    return all_passed


def main():
    """Main entry point."""
    # Find the server files
    script_dir = Path(__file__).parent
    repo_root = script_dir.parent

    main_file = repo_root / "server.py"
    dev_file = repo_root / "server_local.py"

    if not main_file.exists():
        print(f"❌ Main server file not found: {main_file}")
        return 1

    if not dev_file.exists():
        print(f"❌ Dev server file not found: {dev_file}")
        return 1

    main_content = main_file.read_text()
    dev_content = dev_file.read_text()

    print("=" * 50)
    print("Server File Sync Verification")
    print("=" * 50)
    print(f"Main: {main_file.name}")
    print(f"Dev:  {dev_file.name}")

    all_passed = True

    # Check manage_trading_partner (has CREATE and UPDATE)
    all_passed = check_function(
        main_content, dev_content,
        "manage_trading_partner",
        check_create=True,
        check_update=True
    ) and all_passed

    # Check manage_organization (has CREATE and UPDATE)
    all_passed = check_function(
        main_content, dev_content,
        "manage_organization",
        check_create=True,
        check_update=True
    ) and all_passed

    # Check manage_process (uses YAML, no CREATE/UPDATE sections)
    all_passed = check_function(
        main_content, dev_content,
        "manage_process",
        check_create=False,
        check_update=False
    ) and all_passed

    print("\n" + "=" * 50)
    if all_passed:
        print("All checks passed! ✅")
        print("=" * 50)
        return 0
    else:
        print("❌ Files are out of sync!")
        print("Fix the differences listed above before committing.")
        print("=" * 50)
        return 1


if __name__ == "__main__":
    sys.exit(main())
