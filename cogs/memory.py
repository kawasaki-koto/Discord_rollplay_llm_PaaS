from discord.ext import commands
from utils.console_display import log_success
from utils import db_manager as data_manager # ★ db_manager に変更

class MemoryCog(commands.Cog, name="MemoryCog"):
    def __init__(self, bot):
        self.bot = bot
        self.memories = data_manager.get_data('memory') # db_managerから取得
        log_success("MEMORY", f"{len(self.memories)}件の記憶を読み込みました。")

    def add_memory(self, memory_text: str):
        self.memories.append(memory_text)
        log_success("MEMORY", f"新しい記憶をメモリに追加: {memory_text}")
        data_manager.save_data('memory', self.memories) # ★ DB保存

    def get_memories(self) -> list:
        return self.memories
    
    def delete_memory(self, index: int): 
        if 0 <= index < len(self.memories):
            removed_memory = self.memories.pop(index)
            log_success("MEMORY", f"記憶 No.{index+1} をメモリから削除しました。")
            data_manager.save_data('memory', self.memories) # ★ DB保存
            return removed_memory
        return None

    def reset_memories(self):
        self.memories.clear()
        log_success("MEMORY", "メモリ上の記憶データがリセットされました。")
        data_manager.save_data('memory', self.memories) # ★ DB保存

async def setup(bot):
    await bot.add_cog(MemoryCog(bot))