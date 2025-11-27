use spacetimedb::{table, reducer, Table, ReducerContext, Timestamp, ScheduleAt, SpacetimeType};
use std::time::Duration;

// ============================================
// Original tables (comments benchmark)
// ============================================

#[table(name = users, public)]
pub struct User {
    #[primary_key]
    #[auto_inc]
    pub id: u64,
    pub name: String,
    pub email: String,
    pub created_at: Timestamp,
}

#[table(name = galleries, public)]
pub struct Gallery {
    #[primary_key]
    #[auto_inc]
    pub id: u64,
    #[index(btree)]
    pub user_id: u64,
    pub title: String,
    pub description: String,
    pub created_at: Timestamp,
}

#[table(name = comments, public)]
pub struct Comment {
    #[primary_key]
    #[auto_inc]
    pub id: u64,
    #[index(btree)]
    pub gallery_id: u64,
    #[index(btree)]
    pub user_id: u64,
    pub text: String,
    pub created_at: Timestamp,
}

// ============================================
// Game Loop Tables - Batched World State
// ============================================

// Individual player state (for internal tracking, not subscribed by clients)
#[derive(SpacetimeType, Clone, Debug)]
pub struct PlayerData {
    pub player_id: u64,
    pub x: f32,
    pub y: f32,
    pub z: f32,
    pub rotation: f32,
    pub velocity_x: f32,
    pub velocity_y: f32,
    pub velocity_z: f32,
    pub health: u32,
    pub last_input_tick: u64,
}

// Single world state table - ONE row updated 120 times/sec
// Clients subscribe to this single row and receive batched updates
#[table(name = world_state, public)]
pub struct WorldState {
    #[primary_key]
    pub id: u32,  // Always 0 for singleton
    pub tick: u64,
    pub server_time_ms: u64,
    pub player_count: u32,
    // Serialized player data - all players in one blob
    pub players_data: Vec<PlayerData>,
    pub last_update: Timestamp,
}

// Player input buffer - stores pending inputs until next tick
#[table(name = player_inputs, public)]
pub struct PlayerInput {
    #[primary_key]
    pub player_id: u64,
    pub x: f32,
    pub y: f32,
    pub z: f32,
    pub rotation: f32,
    pub velocity_x: f32,
    pub velocity_y: f32,
    pub velocity_z: f32,
    pub input_tick: u64,
}

// Scheduled reducer timer table - controls the game loop
#[table(name = game_loop_timer, public, scheduled(game_tick))]
pub struct GameLoopTimer {
    #[primary_key]
    #[auto_inc]
    pub scheduled_id: u64,
    pub scheduled_at: ScheduleAt,
}

// ============================================
// Original reducers
// ============================================

#[reducer]
pub fn create_user(ctx: &ReducerContext, name: String, email: String) -> Result<(), String> {
    ctx.db.users().insert(User {
        id: 0,
        name,
        email,
        created_at: ctx.timestamp,
    });
    Ok(())
}

#[reducer]
pub fn create_gallery(ctx: &ReducerContext, user_id: u64, title: String, description: String) -> Result<(), String> {
    ctx.db.galleries().insert(Gallery {
        id: 0,
        user_id,
        title,
        description,
        created_at: ctx.timestamp,
    });
    Ok(())
}

#[reducer]
pub fn add_comment(ctx: &ReducerContext, gallery_id: u64, user_id: u64, text: String) -> Result<(), String> {
    ctx.db.comments().insert(Comment {
        id: 0,
        gallery_id,
        user_id,
        text,
        created_at: ctx.timestamp,
    });
    Ok(())
}

#[reducer]
pub fn get_comments_for_gallery(ctx: &ReducerContext, gallery_id: u64) -> Result<(), String> {
    let _comments: Vec<_> = ctx.db.comments().gallery_id().filter(&gallery_id).collect();
    Ok(())
}

// ============================================
// Game Loop reducers - Batched Architecture
// ============================================

// Called by players to submit their input (buffered until next tick)
#[reducer]
pub fn submit_input(
    ctx: &ReducerContext,
    player_id: u64,
    x: f32,
    y: f32,
    z: f32,
    rotation: f32,
    velocity_x: f32,
    velocity_y: f32,
    velocity_z: f32,
) -> Result<(), String> {
    // Get current tick
    let current_tick = ctx.db.world_state().id().find(&0)
        .map(|ws| ws.tick)
        .unwrap_or(0);

    // Update or insert player input (overwrites previous input if multiple per tick)
    if ctx.db.player_inputs().player_id().find(&player_id).is_some() {
        ctx.db.player_inputs().player_id().update(PlayerInput {
            player_id,
            x,
            y,
            z,
            rotation,
            velocity_x,
            velocity_y,
            velocity_z,
            input_tick: current_tick,
        });
    } else {
        ctx.db.player_inputs().insert(PlayerInput {
            player_id,
            x,
            y,
            z,
            rotation,
            velocity_x,
            velocity_y,
            velocity_z,
            input_tick: current_tick,
        });
    }
    Ok(())
}

// Server game loop - runs at 120Hz
// Collects all player inputs, applies physics, produces ONE batched world state update
#[reducer]
pub fn game_tick(ctx: &ReducerContext, _timer: GameLoopTimer) -> Result<(), String> {
    // Get current world state
    let world = ctx.db.world_state().id().find(&0);

    let (current_tick, mut players) = match world {
        Some(ws) => (ws.tick + 1, ws.players_data),
        None => (1, Vec::new()),
    };

    // Collect all pending inputs
    let inputs: Vec<PlayerInput> = ctx.db.player_inputs().iter().collect();

    // Apply inputs to player states
    for input in &inputs {
        // Find existing player or skip (player must join first)
        if let Some(idx) = players.iter().position(|p| p.player_id == input.player_id) {
            players[idx].x = input.x;
            players[idx].y = input.y;
            players[idx].z = input.z;
            players[idx].rotation = input.rotation;
            players[idx].velocity_x = input.velocity_x;
            players[idx].velocity_y = input.velocity_y;
            players[idx].velocity_z = input.velocity_z;
            players[idx].last_input_tick = current_tick;
        }
    }

    // Apply physics to all players
    let dt = 1.0 / 120.0; // Delta time for 120Hz
    let damping = 0.98_f32;

    for player in &mut players {
        // Simple physics: position += velocity * dt
        player.x += player.velocity_x * dt;
        player.y += player.velocity_y * dt;
        player.z += player.velocity_z * dt;

        // Apply friction/damping
        player.velocity_x *= damping;
        player.velocity_y *= damping;
        player.velocity_z *= damping;
    }

    // Update world state - this is the SINGLE update all clients receive
    let player_count = players.len() as u32;

    if ctx.db.world_state().id().find(&0).is_some() {
        ctx.db.world_state().id().update(WorldState {
            id: 0,
            tick: current_tick,
            server_time_ms: current_tick * 8,
            player_count,
            players_data: players,
            last_update: ctx.timestamp,
        });
    } else {
        ctx.db.world_state().insert(WorldState {
            id: 0,
            tick: current_tick,
            server_time_ms: current_tick * 8,
            player_count,
            players_data: players,
            last_update: ctx.timestamp,
        });
    }

    Ok(())
}

// Start the game loop (call once to begin)
#[reducer]
pub fn start_game_loop(ctx: &ReducerContext) -> Result<(), String> {
    // Check if game loop is already running
    if ctx.db.game_loop_timer().iter().next().is_some() {
        return Err("Game loop already running".to_string());
    }

    // Initialize or reset world state
    if ctx.db.world_state().id().find(&0).is_some() {
        ctx.db.world_state().id().update(WorldState {
            id: 0,
            tick: 0,
            server_time_ms: 0,
            player_count: 0,
            players_data: Vec::new(),
            last_update: ctx.timestamp,
        });
    } else {
        ctx.db.world_state().insert(WorldState {
            id: 0,
            tick: 0,
            server_time_ms: 0,
            player_count: 0,
            players_data: Vec::new(),
            last_update: ctx.timestamp,
        });
    }

    // Clear any stale inputs
    let old_inputs: Vec<_> = ctx.db.player_inputs().iter().collect();
    for input in old_inputs {
        ctx.db.player_inputs().player_id().delete(&input.player_id);
    }

    // Start the 120Hz game loop (~8.33ms interval)
    ctx.db.game_loop_timer().insert(GameLoopTimer {
        scheduled_id: 0,
        scheduled_at: ScheduleAt::Interval(Duration::from_micros(8333).into()),
    });

    log::info!("Game loop started at 120Hz (8.33ms interval)");
    Ok(())
}

// Stop the game loop
#[reducer]
pub fn stop_game_loop(ctx: &ReducerContext) -> Result<(), String> {
    // Remove all timer entries to stop the loop
    let timers: Vec<_> = ctx.db.game_loop_timer().iter().collect();
    for timer in timers {
        ctx.db.game_loop_timer().scheduled_id().delete(&timer.scheduled_id);
    }
    log::info!("Game loop stopped");
    Ok(())
}

// Join as a player
#[reducer]
pub fn join_game(ctx: &ReducerContext, player_id: u64) -> Result<(), String> {
    // Get current world state
    let world = ctx.db.world_state().id().find(&0);

    let mut players = match &world {
        Some(ws) => ws.players_data.clone(),
        None => Vec::new(),
    };

    // Check if player already exists
    if players.iter().any(|p| p.player_id == player_id) {
        return Err("Player already joined".to_string());
    }

    // Add new player
    players.push(PlayerData {
        player_id,
        x: 0.0,
        y: 0.0,
        z: 0.0,
        rotation: 0.0,
        velocity_x: 0.0,
        velocity_y: 0.0,
        velocity_z: 0.0,
        health: 100,
        last_input_tick: 0,
    });

    // Create input buffer entry for this player
    ctx.db.player_inputs().insert(PlayerInput {
        player_id,
        x: 0.0,
        y: 0.0,
        z: 0.0,
        rotation: 0.0,
        velocity_x: 0.0,
        velocity_y: 0.0,
        velocity_z: 0.0,
        input_tick: 0,
    });

    // Update world state
    let player_count = players.len() as u32;
    if let Some(ws) = world {
        ctx.db.world_state().id().update(WorldState {
            id: 0,
            tick: ws.tick,
            server_time_ms: ws.server_time_ms,
            player_count,
            players_data: players,
            last_update: ctx.timestamp,
        });
    } else {
        ctx.db.world_state().insert(WorldState {
            id: 0,
            tick: 0,
            server_time_ms: 0,
            player_count,
            players_data: players,
            last_update: ctx.timestamp,
        });
    }

    Ok(())
}

// Leave the game
#[reducer]
pub fn leave_game(ctx: &ReducerContext, player_id: u64) -> Result<(), String> {
    // Remove from world state
    if let Some(ws) = ctx.db.world_state().id().find(&0) {
        let players: Vec<PlayerData> = ws.players_data.into_iter()
            .filter(|p| p.player_id != player_id)
            .collect();
        let player_count = players.len() as u32;

        ctx.db.world_state().id().update(WorldState {
            id: 0,
            tick: ws.tick,
            server_time_ms: ws.server_time_ms,
            player_count,
            players_data: players,
            last_update: ctx.timestamp,
        });
    }

    // Remove input buffer entry
    ctx.db.player_inputs().player_id().delete(&player_id);

    Ok(())
}

// Initialize with test data
#[reducer(init)]
pub fn init(ctx: &ReducerContext) {
    // Create benchmark user
    ctx.db.users().insert(User {
        id: 0,
        name: "Benchmark User".to_string(),
        email: "bench@test.com".to_string(),
        created_at: ctx.timestamp,
    });

    // Create benchmark gallery
    ctx.db.galleries().insert(Gallery {
        id: 0,
        user_id: 1,
        title: "Benchmark Gallery".to_string(),
        description: "Gallery for benchmark testing".to_string(),
        created_at: ctx.timestamp,
    });

    // Initialize empty world state
    ctx.db.world_state().insert(WorldState {
        id: 0,
        tick: 0,
        server_time_ms: 0,
        player_count: 0,
        players_data: Vec::new(),
        last_update: ctx.timestamp,
    });
}
