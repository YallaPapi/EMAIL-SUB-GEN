import threading


class _Counters:
    def __init__(self):
        self.lock = threading.Lock()
        self.openai_input = 0
        self.openai_output = 0
        self.perplexity_input = 0
        self.perplexity_output = 0

    def add_openai(self, input_tokens: int, output_tokens: int):
        with self.lock:
            self.openai_input += max(0, int(input_tokens or 0))
            self.openai_output += max(0, int(output_tokens or 0))

    def add_perplexity(self, prompt_tokens: int, completion_tokens: int):
        with self.lock:
            self.perplexity_input += max(0, int(prompt_tokens or 0))
            self.perplexity_output += max(0, int(completion_tokens or 0))

    def snapshot(self) -> dict:
        with self.lock:
            return {
                "openai_input": self.openai_input,
                "openai_output": self.openai_output,
                "perplexity_input": self.perplexity_input,
                "perplexity_output": self.perplexity_output,
            }


COUNTERS = _Counters()

