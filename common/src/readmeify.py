import sys

class State(object):
  START = 1
  MAKE_PYTHON = 2
  END_PYTHON = 3

def main():
  state = State.START
  output = []
  for line in sys.stdin:
    if state == State.START and '"""' in line:
      state = State.MAKE_PYTHON # Ignore the first """.
    elif state == State.MAKE_PYTHON and '"""' in line:
      output.append('```python')
      state = State.END_PYTHON
    elif state == State.END_PYTHON and '#' in line:
      output.append('```')
      output.append(line.rstrip())
      state = State.START
    elif state == State.END_PYTHON and '"""' in line:
      output.append('```')
      state = State.MAKE_PYTHON
    else:
      output.append(line.rstrip())
  output.append('```')

  remove_next = False
  # Cleanup pass
  for first, second in zip(output, output[1:] + ['']):
    if remove_next:
      remove_next = False
      continue
    elif first.strip() == '' and second == '```':
      continue
    elif first == '```python' and second == '```':
      remove_next = True
      continue
    print first

if __name__ == '__main__':
  main()
