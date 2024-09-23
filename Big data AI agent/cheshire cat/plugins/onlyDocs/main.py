from cat.mad_hatter.decorators import hook


@hook
def before_cat_sends_message(message, cat):
    doc=message.why.memory["declarative"]
    if(len(doc)==0):
        message.content="sorry, i don't have documentation about your question"
    return message