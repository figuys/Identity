from pathlib import Path


class PathManager:
	"""
	A class to manage file paths for a data processing pipeline.

	It standardizes the generation of output, temporary, error, and
	next-stage source file paths based on an initial source file.
	It also handles the creation of the required directories.
	"""

	def __init__(self, src_file: str, base_dir: str = r'D:\GitHub\fiGuys\Identity'):
		"""
		Initializes the PathManager with a source file path.

		Args:
			src_file (str): The full path to the source file.
			base_dir (str): The root directory of the project.
		"""
		self._base_dir = Path(base_dir)
		self.src = Path(src_file)

		try:
			relative_path = self.src.relative_to(self._base_dir)
			# e.g., parts for 'D:/.../01/src/file.csv' are ('01', 'src', 'file.csv')
			self._step = relative_path.parts[0]
			self._filename = self.src.name
		except ValueError:
			raise ValueError(f"src_file '{src_file}' is not inside base_dir '{base_dir}'")

		self._out_dir = self._base_dir / self._step / 'out'
		self._tmp_dir = self._base_dir / self._step / 'tmp'
		self._err_dir = self._base_dir / self._step / 'err'

		# Determine next step number for the 'new' path
		try:
			next_step_num = int(self._step) + 1
			self._next_step = f'{next_step_num:02d}'
			self._new_dir = self._base_dir / self._next_step / 'src'
		except (ValueError, TypeError):
			# Handle cases where step is not a number
			self._next_step = self._step
			self._new_dir = self._base_dir / self._next_step / 'src'

	def _get_path(self, directory: Path, extension: str | None = '.parquet') -> Path:
		"""Helper to construct a path with a new extension."""
		if extension:
			return directory / self.src.with_suffix(extension).name
		return directory / self.src.name

	@property
	def out(self) -> Path:
		return self._get_path(self._out_dir)

	@property
	def tmp(self) -> Path:
		return self._get_path(self._tmp_dir)

	@property
	def err(self) -> Path:
		return self._get_path(self._err_dir)

	@property
	def new(self) -> Path:
		return self._get_path(self._new_dir)

	def ensure_dirs(self) -> None:
		"""Creates all necessary output directories."""
		for directory in [self._out_dir, self._tmp_dir, self._err_dir, self._new_dir]:
			directory.mkdir(parents=True, exist_ok=True)
