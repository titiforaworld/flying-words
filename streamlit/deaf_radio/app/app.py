import streamlit as st
import streamlit.components.v1 as components


# page conf
st.set_page_config(
    page_title="Flying Words",
    layout="centered")
st.title('Radio Player')

HTML = """

<style>
html,
body {
    height: 100%;
}

body {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    color: white;
}

audio {
    margin-bottom: 24px;
}

video {
    max-width: 80%;
}

</style>


<audio class="player" src="https://www.codepunker.com/resources/audio-sync-with-text/10bears.mp3" controls></audio>

<div class="lyrics" style="display: none">
  0.125 | There were
  0.485 | 10 in his bed
  1.685 | and the little
  2.245 | one said
  2.985 | Roll over!
  5.405 |
</div>


<script language="javascript">

const player = document.querySelector('.player')
const lyrics = document.querySelector('.lyrics')
const lines = lyrics.textContent.trim().split('\\n')

lyrics.removeAttribute('style')
lyrics.innerText = ''

let syncData = []

lines.map((line, index) => {
    const [time, text] = line.trim().split('|')
    syncData.push({'start': time.trim(), 'text': text.trim()})
})

player.addEventListener('timeupdate', () => {
    syncData.forEach((item) => {
        if (player.currentTime >= item.start)
        {
            lyrics.innerText = item.text
        }
    })
})

</script>

"""



components.html(HTML)
