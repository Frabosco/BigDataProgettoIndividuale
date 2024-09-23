from cat.mad_hatter.decorators import hook


@hook(priority = 3)
def agent_prompt_prefix(prefix, cat):
    prefix="""you are an expert ai agent in animation topic(animated movie and series) and help people about this topic"""
    return prefix