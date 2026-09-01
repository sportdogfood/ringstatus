const goals = [
  { n: "01", title: "Build a useful record", body: "Keep the horses, classes, video and decisions that explain the work—not just the result." },
  { n: "02", title: "Show the range", body: "Let coaches see how the riding changes across horses, rings and responsibilities." }
];

const horses = [
  ["Favorite horse 01", "Hunter • Equitation", "The rides that taught patience, feel and preparation."],
  ["Favorite horse 02", "Jumper • Medal", "A different question, a different kind of confidence."],
  ["Favorite horse 03", "Barn favorite", "The horse behind the story still comes first."]
];

const videos = [
  ["Round study 01", "Competition video"],
  ["Training note 02", "Flatwork"],
  ["What changed 03", "Ride review"]
];

export default function Home() {
  return (
    <main>
      <nav className="lp-nav">
        <a className="brand" href="#top"><span>L</span> Lainey Posa</a>
        <div className="navlinks">
          <a href="#classes">Classes</a><a href="#horses">Horses</a>
          <a href="#me">Me</a><a href="#videos">Videos</a><a href="#dashboard">Dashboard</a>
        </div>
      </nav>

      <section id="top" className="section hero-wrap">
        <p className="eyebrow">01 / START</p>
        <div className="lane-panel">
          <p className="kicker">Lainey Posa • Equestrian</p>
          <h1>The work behind every round.</h1>
          <p className="lede">A practical record of horses, classes, video, goals and the small decisions that shape the next ride.</p>
          <a className="button" href="#dashboard">Browse my dashboard <span>↗</span></a>
        </div>
      </section>

      <section id="me" className="section">
        <div className="section-head"><p className="eyebrow">02 / GOALS</p><h2>What I’m working toward.</h2></div>
        <div className="goal-grid">
          {goals.map(g => <article className="subsection" key={g.n}><span>{g.n}</span><h3>{g.title}</h3><p>{g.body}</p></article>)}
        </div>
      </section>

      <section id="horses" className="section ruled">
        <div className="section-head split"><div><p className="eyebrow">03 / HORSES</p><h2>Favorites, for a reason.</h2></div><p>Not a ranking. A working set of horses and the lessons attached to them.</p></div>
        <div className="card-row">
          {horses.map((h,i) => <article className="carousel-card horse" key={h[0]}><div className="card-image"><span>0{i+1}</span></div><p className="kicker">{h[1]}</p><h3>{h[0]}</h3><p>{h[2]}</p></article>)}
        </div>
      </section>

      <section id="videos" className="section ruled">
        <div className="section-head split"><div><p className="eyebrow">04 / VIDEO</p><h2>Watch the work change.</h2></div><p>Rounds and training notes will be added in real time.</p></div>
        <div className="card-row">
          {videos.map((v,i) => <article className="carousel-card video" key={v[0]}><div className="video-frame"><span>▶</span><small>0{i+1}:24</small></div><p className="kicker">{v[1]}</p><h3>{v[0]}</h3></article>)}
        </div>
      </section>

      <section id="classes" className="section compact">
        <div className="subsection wide"><span>05</span><p className="eyebrow">CLASSES</p><h2>The record grows with the season.</h2><p>Filters and live class rows will drop into the same section system when the data is ready.</p></div>
      </section>

      <section id="dashboard" className="section dashboard">
        <p className="eyebrow">06 / DASHBOARD</p><h2>Everything in one working view.</h2>
        <p>Horses, classes, goals, transcripts and video—organized for the next conversation.</p>
        <a className="button light" href="#top">Browse my dashboard <span>↗</span></a>
      </section>

      <footer className="lp-footer"><a className="brand" href="#top"><span>L</span> Lainey Posa</a><p>Built around the work. Ready for what comes next.</p><p>© 2026</p></footer>
    </main>
  );
}
