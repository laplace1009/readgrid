PRAGMA foreign_keys = ON;

DROP TABLE IF EXISTS comments;
DROP TABLE IF EXISTS tasks;
DROP TABLE IF EXISTS projects;
DROP TABLE IF EXISTS users;

CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    role TEXT NOT NULL DEFAULT 'developer',
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE projects (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    slug TEXT NOT NULL UNIQUE,
    owner_id INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    repo_url TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (owner_id) REFERENCES users(id)
);

CREATE INDEX idx_projects_owner_id ON projects(owner_id);

CREATE TABLE tasks (
    id INTEGER PRIMARY KEY,
    project_id INTEGER NOT NULL,
    assignee_id INTEGER,
    title TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'todo',
    priority INTEGER NOT NULL DEFAULT 2,
    estimate_hours REAL,
    due_date TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (project_id) REFERENCES projects(id),
    FOREIGN KEY (assignee_id) REFERENCES users(id)
);

CREATE INDEX idx_tasks_project_id ON tasks(project_id);
CREATE INDEX idx_tasks_assignee_id ON tasks(assignee_id);
CREATE INDEX idx_tasks_status_priority ON tasks(status, priority);

CREATE TABLE comments (
    id INTEGER PRIMARY KEY,
    task_id INTEGER NOT NULL,
    author_id INTEGER NOT NULL,
    body TEXT NOT NULL,
    is_internal INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (task_id) REFERENCES tasks(id),
    FOREIGN KEY (author_id) REFERENCES users(id)
);

CREATE INDEX idx_comments_task_id ON comments(task_id);
CREATE INDEX idx_comments_author_id ON comments(author_id);

INSERT INTO users (id, username, display_name, email, role, is_active) VALUES
    (1, 'mina', 'Mina Kim', 'mina@example.com', 'lead', 1),
    (2, 'juno', 'Juno Park', 'juno@example.com', 'developer', 1),
    (3, 'ara', 'Ara Choi', 'ara@example.com', 'developer', 1),
    (4, 'doyun', 'Do-yun Lee', 'doyun@example.com', 'qa', 0);

INSERT INTO projects (id, name, slug, owner_id, status, repo_url) VALUES
    (1, 'ReadGrid TUI', 'readgrid-tui', 1, 'active', 'https://example.com/readgrid'),
    (2, 'Telemetry Cleanup', 'telemetry-cleanup', 2, 'planning', NULL),
    (3, 'Docs Refresh', 'docs-refresh', 1, 'active', 'https://example.com/docs');

INSERT INTO tasks (id, project_id, assignee_id, title, status, priority, estimate_hours, due_date) VALUES
    (1, 1, 2, 'Wire up schema browser', 'in_progress', 1, 6.5, '2026-04-01'),
    (2, 1, 3, 'Render relationship panel', 'todo', 1, 4.0, '2026-04-03'),
    (3, 1, NULL, 'Add sample data preview paging', 'todo', 2, 3.5, NULL),
    (4, 2, 2, 'Audit event payload fields', 'done', 2, 2.0, '2026-03-20'),
    (5, 3, 4, 'Rewrite contributor quickstart', 'review', 3, 1.5, '2026-03-29');

INSERT INTO comments (id, task_id, author_id, body, is_internal) VALUES
    (1, 1, 1, 'Start with keyboard-first navigation and simple panes.', 1),
    (2, 1, 2, 'Initial browser state is in place, detail panel next.', 0),
    (3, 2, 3, 'Need incoming and outgoing FK labels in the UI.', 0),
    (4, 3, 1, 'Keep page size fixed at 50 rows for v1.', 1),
    (5, 4, 2, 'Telemetry payload review is complete.', 0),
    (6, 5, 4, 'Docs draft is ready for product review.', 0);
