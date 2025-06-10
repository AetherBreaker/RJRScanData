from itertools import batched
from typing import Any, Callable, Literal, Optional, Self, TextIO

from logging_config import RICH_CONSOLE
from rich import get_console
from rich.console import Console, RenderableType
from rich.control import Control
from rich.live import Live
from rich.live_render import VerticalOverflowMethod
from rich.panel import Panel
from rich.progress import (
  BarColumn,
  MofNCompleteColumn,
  Progress,
  ProgressColumn,
  Task,
  TaskProgressColumn,
  TextColumn,
  TimeRemainingColumn,
)
from rich.prompt import DefaultType, InvalidResponse, PromptBase, PromptType
from rich.segment import ControlType, Segment
from rich.table import Table
from rich.text import Text, TextType

type RemainingItemsIDType = int
type RemainingItemsDisplayType = str
type RemainingTitleType = str


class ChoicePrompt(PromptBase[int]):
  tab_length = 2

  response_type = int
  validate_error_message = "[prompt.invalid]Please enter a valid integer number that corresponds to one of the choices."
  choices: dict[int, Any]

  def __init__(
    self,
    prompt: TextType,
    choices: list[str],
    *,
    console: Console | None = None,
    show_default: bool = True,
    show_choices: bool = True,
    allow_multiple_choices: bool | list[str] = False,
  ) -> None:
    self.console = console or get_console()
    self.prompt = Text.from_markup(prompt, style="prompt", end="\n") if isinstance(prompt, str) else prompt
    self.choices = {choice_int: choice for choice_int, choice in enumerate(choices, start=1)}
    self.show_default = show_default
    self.show_choices = show_choices

    self.allow_multiple_choices = bool(allow_multiple_choices)
    self.allowed_choices = allow_multiple_choices if isinstance(allow_multiple_choices, list) else None

  def make_prompt(self, default: DefaultType) -> Text:
    """Make prompt text.

    Args:
        default (DefaultType): Default value.

    Returns:
        Text: Text to display in prompt.
    """
    prompt = self.prompt.copy()

    if default != ... and self.show_default and isinstance(default, (str, self.response_type)):
      prompt.append(" ")
      _default = self.render_default(default)
      prompt.append(_default)

    prompt.append(self.prompt_suffix)

    if self.show_choices:
      _choices = f"\n{self.tab_length*" "}".join(f"[{i}]: {choice}" for i, choice in self.choices.items())
      choices = f'\n{self.tab_length * " "}{_choices}\n'
      prompt.append(choices, "prompt.choices")

    return prompt

  def check_choice(self, value: str) -> bool:
    """Check value is in the list of valid choices.

    Args:
        value (str): Value entered by user.

    Returns:
        bool: True if choice was valid, otherwise False.
    """
    assert self.choices is not None
    if self.allow_multiple_choices:
      if len(value) > 1 and self.allowed_choices:
        return all(int(i) in self.allowed_choices for i in value)
      else:
        return all(int(i) in self.choices for i in value)
    return int(value) in self.choices

  def process_response(self, value: str) -> PromptType | list[PromptType]:
    """Process response from user, convert to prompt type.

    Args:
        value (str): String typed by user.

    Raises:
        InvalidResponse: If ``value`` is invalid.

    Returns:
        PromptType: The value to be returned from ask method.
    """
    value = value.strip()
    try:
      return_value: PromptType = self.response_type(value)
    except ValueError as e:
      raise InvalidResponse(self.validate_error_message) from e

    if self.choices is not None and not self.check_choice(value):
      raise InvalidResponse(self.illegal_choice_message)

    return_value = [self.choices[int(i)] for i in value] if self.allow_multiple_choices else self.choices[int(return_value)]

    return return_value

  @classmethod
  def ask(
    cls,
    prompt: TextType,
    choices: list[str],
    *,
    console: Console | None = None,
    show_default: bool = True,
    show_choices: bool = True,
    allow_multiple_choices: bool = False,
    default: Any = ...,
    stream: Optional[TextIO] = None,
  ) -> PromptType:
    """Shortcut to construct and run a prompt loop and return the result.

    Example:
        >>> filename = Prompt.ask("Enter a filename")

    Args:
        prompt (TextType, optional): Prompt text. Defaults to "".
        console (Console, optional): A Console instance or None to use global console. Defaults to None.
        password (bool, optional): Enable password input. Defaults to False.
        choices (List[str], optional): A list of valid choices. Defaults to None.
        case_sensitive (bool, optional): Matching of choices should be case-sensitive. Defaults to True.
        show_default (bool, optional): Show default in prompt. Defaults to True.
        show_choices (bool, optional): Show choices in prompt. Defaults to True.
        stream (TextIO, optional): Optional text file open for reading to get input. Defaults to None.
    """
    _prompt = cls(
      prompt,
      console=console,
      choices=choices,
      show_default=show_default,
      show_choices=show_choices,
      allow_multiple_choices=allow_multiple_choices,
    )
    return _prompt(default=default, stream=stream)

  def __call__(self, *, default: Any = ..., stream: Optional[TextIO] = None) -> Any:
    """Run the prompt loop.

    Args:
        default (Any, optional): Optional default value.

    Returns:
        PromptType: Processed value.
    """
    while True:
      self.pre_prompt()
      prompt = self.make_prompt(default)
      style = self.console.get_style("prompt")
      self._shape = Segment.get_shape(self.console.render_lines(prompt, self.console.options, style=style, pad=False))
      value = self.get_input(self.console, prompt, False, stream=stream)
      self.clear_prompt()
      if value == "" and default != ...:
        return default
      try:
        return_value = self.process_response(value)
      except InvalidResponse as error:
        self.on_validate_error(value, error)
        continue
      else:
        return return_value

  def clear_prompt(self) -> None:
    if self._shape is not None:
      _, height = self._shape
      ctrl = Control(
        ControlType.CARRIAGE_RETURN,
        *((ControlType.CURSOR_UP, 1), (ControlType.ERASE_IN_LINE, 2)) * height,
      )
    else:
      ctrl = Control()
    self.console.control(ctrl)


class RemainingColumn(ProgressColumn):
  last_desc = ""

  def __init__(
    self,
    title: str,
    items: dict[RemainingItemsIDType, RemainingItemsDisplayType] | list[RemainingItemsIDType | RemainingItemsDisplayType],
    vertical_padding: int = 0,
    horizontal_padding: int = 1,
    *args,
    **kwargs,
  ):
    self.title = title
    self.items = {k: str(v) for k, v in items.items()} if isinstance(items, dict) else {i: str(i) for i in items}
    self.render_items = {k: str(v) for k, v in items.items()} if isinstance(items, dict) else {i: str(i) for i in items}
    self.num_cols = 6
    self._is_empty = False
    self.vertical_padding = vertical_padding
    self.horizontal_padding = horizontal_padding

    self.max_width = max(map(lambda x: len(str(x)), items.values()))

    super().__init__(*args, **kwargs)

  def update_items(self, *items_to_remove: tuple[RemainingItemsIDType]) -> None:
    if items_to_remove != [""]:
      for item in items_to_remove:
        if item in self.items:
          self.items[item] = None
        else:
          pass

    self._is_empty = all(item is None for item in self.items.values())

    self.update_render_items()

  def update_render_items(self):
    for key, val in self.items.items():
      self.render_items[key] = " " * self.max_width if val is None else val

  def render(self, task: "Task") -> RenderableType:
    if self.vertical_padding == 0:
      vert_pad = 1 if any("\n" in item for item in self.render_items.values()) else 0
    else:
      vert_pad = self.vertical_padding

    remaining_grid = Table.grid(
      padding=(vert_pad, self.horizontal_padding),
      # expand=True,
    )

    for row in batched(self.render_items.values(), self.num_cols):
      remaining_grid.add_row(*row)

    return Panel.fit(remaining_grid, title=self.title, border_style="blue", highlight=True)

  @property
  def is_empty(self) -> bool:
    return self._is_empty


class LiveCustom(Live):
  def __init__(
    self,
    *,
    console: Optional[Console] = RICH_CONSOLE,
    screen: bool = False,
    auto_refresh: bool = True,
    refresh_per_second: float = 4,
    transient: bool = False,
    redirect_stdout: bool = True,
    redirect_stderr: bool = True,
    vertical_overflow: VerticalOverflowMethod = "ellipsis",
    get_renderable: Optional[Callable[[], RenderableType]] = None,
  ) -> None:
    pbar = Progress(
      BarColumn(),
      TaskProgressColumn(),
      MofNCompleteColumn(),
      TimeRemainingColumn(),
      TextColumn("[progress.description]{task.description}"),
      console=RICH_CONSOLE,
    )
    self.pbar = pbar

    display_table = Table.grid()
    display_table.add_row(pbar)

    super().__init__(
      display_table,
      console=console,
      screen=screen,
      auto_refresh=auto_refresh,
      refresh_per_second=refresh_per_second,
      transient=transient,
      redirect_stdout=redirect_stdout,
      redirect_stderr=redirect_stderr,
      vertical_overflow=vertical_overflow,
      get_renderable=get_renderable,
    )

  def wrap_update_remaining(self, rem_col_key: int) -> Callable[[int], None]:
    rem = self.remaining_cols[rem_col_key]

    def wrapper(*storenums: tuple[int | str]) -> None:
      rem.update_items(*storenums)
      if rem._is_empty:
        self.remove_remaining(rem_col_key)

    return wrapper

  def init_remaining(
    self,
    *args: tuple[tuple[dict[RemainingItemsIDType, RemainingItemsDisplayType], RemainingTitleType]],
    performant: bool = True,
  ) -> tuple[Callable[[RemainingItemsIDType], None]]:  # sourcery skip: class-extract-method
    self.remaining_pbars: dict[int, Progress] = {}
    self.remaining_cols: dict[int, RemainingColumn] = {}

    update_callables = []

    for index, (remaining_items, title) in enumerate(args):
      rem_col = RemainingColumn(title, remaining_items)
      remaining_pbar = Progress(
        rem_col,
        console=RICH_CONSOLE,
      )

      remaining_pbar.add_task(description=title, total=len(remaining_items))

      self.remaining_pbars[index] = remaining_pbar
      self.remaining_cols[index] = rem_col

      update_callables.append(
        self.wrap_update_remaining(index),
      )

    self.display_method: Literal["pretty", "performant", "single"] = ...

    if len(self.remaining_pbars) == 1:
      self.display_method = "single"
    elif performant:
      self.display_method = "performant"
    else:
      self.display_method = "pretty"

    self.update_display()

    return tuple(update_callables) if len(update_callables) > 1 else update_callables[0]

  def update_display(self):
    display_table = Table.grid()

    match self.display_method:
      case "pretty":
        sub_table = Table.grid()
        sub_table.add_row(*self.remaining_pbars.values())
        display_table.add_row(
          Panel.fit(
            sub_table,
            title="Remaining",
            border_style="blue",
          ),
        )
      case "performant":
        sub_table = Table.grid()
        sub_table.add_row(*self.remaining_pbars.values())
        display_table.add_row(sub_table)
      case "single":
        display_table.add_row(*self.remaining_pbars.values())

    display_table.add_row(self.pbar)

    self.update(display_table, refresh=True)

  def remove_remaining(self, key: int):
    self.remaining_pbars.pop(key)
    self.remaining_cols.pop(key)
    self.update_display()

  def clear_remaining(self):
    self.remaining_pbars = None
    self.remaining_tasks = None
    display_table = Table.grid()
    display_table.add_row(self.pbar)
    self.update(display_table, refresh=True)

  def __enter__(self) -> Self:
    self.start(refresh=self._renderable is not None)
    return self
