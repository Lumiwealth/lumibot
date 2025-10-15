#!/usr/bin/env python3
"""Remove _THETA_PARITY_DEBUG conditional guards and fix indentation."""

import sys

def process_file(filepath):
    with open(filepath, 'r') as f:
        lines = f.readlines()

    output = []
    i = 0

    while i < len(lines):
        line = lines[i]

        # Skip the _THETA_PARITY_DEBUG declaration line
        if '_THETA_PARITY_DEBUG = os.getenv' in line:
            i += 1
            continue

        # Check for if _THETA_PARITY_DEBUG: conditional
        if 'if _THETA_PARITY_DEBUG:' in line:
            # Get the indentation level of the if statement
            if_indent = len(line) - len(line.lstrip())
            i += 1  # Skip the if line

            # Process the indented block
            while i < len(lines):
                block_line = lines[i]

                # Empty lines pass through
                if not block_line.strip():
                    output.append(block_line)
                    i += 1
                    continue

                # Get current line's indentation
                curr_indent = len(block_line) - len(block_line.lstrip())

                # If line is at or before the if indent, we're done with the block
                if curr_indent <= if_indent:
                    break

                # Dedent by 4 spaces (one level)
                output.append(block_line[4:])
                i += 1
        else:
            output.append(line)
            i += 1

    with open(filepath, 'w') as f:
        f.writelines(output)

    print(f"Processed {filepath}")

if __name__ == '__main__':
    file_path = 'lumibot/backtesting/thetadata_backtesting_polars.py'
    process_file(file_path)

    file_path2 = 'lumibot/data_sources/polars_mixin.py'
    process_file(file_path2)
