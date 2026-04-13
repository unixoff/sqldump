mod app;
mod cli;
mod provider;

use app::App;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut app = App::new()?;
    app.run()
}
