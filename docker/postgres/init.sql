-- ==============================================================================
-- News Feed Service Database Initialization Script
-- ==============================================================================

-- 1. Users Table
CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    display_name VARCHAR(100) NOT NULL,
    avatar_url VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 2. Friendships Table (Bidirectional representation)
CREATE TABLE IF NOT EXISTS friendships (
    user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    friend_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    status VARCHAR(20) NOT NULL DEFAULT 'ACCEPTED',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (user_id, friend_id)
);

CREATE INDEX IF NOT EXISTS idx_friendships_friend ON friendships(friend_id, user_id);

-- 3. Posts Table
CREATE TABLE IF NOT EXISTS posts (
    post_id BIGINT PRIMARY KEY,
    author_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    content TEXT NOT NULL,
    media_urls JSONB DEFAULT '[]'::jsonb,
    like_count INT DEFAULT 0,
    comment_count INT DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_posts_author_created ON posts(author_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_posts_created ON posts(created_at DESC);

-- ==============================================================================
-- Mock Data for Development & Testing
-- ==============================================================================

-- Insert Sample Users
INSERT INTO users (user_id, username, display_name, avatar_url) VALUES
(1001, 'alice', 'Alice Johnson', 'https://api.dicebear.com/7.x/avataaars/svg?seed=Alice'),
(1002, 'bob', 'Bob Smith', 'https://api.dicebear.com/7.x/avataaars/svg?seed=Bob'),
(1003, 'charlie', 'Charlie Brown', 'https://api.dicebear.com/7.x/avataaars/svg?seed=Charlie'),
(1004, 'david', 'David Miller', 'https://api.dicebear.com/7.x/avataaars/svg?seed=David'),
(1005, 'eva', 'Eva Davis', 'https://api.dicebear.com/7.x/avataaars/svg?seed=Eva')
ON CONFLICT (user_id) DO NOTHING;

-- Establish Mutual Friendships
-- Alice (1001) is friends with Bob (1002), Charlie (1003), David (1004)
-- Bob (1002) is friends with Alice (1001) and Eva (1005)
INSERT INTO friendships (user_id, friend_id) VALUES
(1001, 1002), (1002, 1001),
(1001, 1003), (1003, 1001),
(1001, 1004), (1004, 1001),
(1002, 1005), (1005, 1002),
(1003, 1004), (1004, 1003)
ON CONFLICT (user_id, friend_id) DO NOTHING;

-- Insert Seed Posts
INSERT INTO posts (post_id, author_id, content, media_urls, like_count, comment_count, created_at) VALUES
(5000000001, 1002, 'Excited to start learning distributed system design today!', '["https://images.unsplash.com/photo-1517694712202-14dd9538aa97"]'::jsonb, 5, 1, NOW() - INTERVAL '3 hours'),
(5000000002, 1003, 'Coffee break with some classic architecture diagrams ☕', '[]'::jsonb, 12, 4, NOW() - INTERVAL '2 hours'),
(5000000003, 1004, 'Just finished setting up Docker Compose with Kafka and Redis!', '[]'::jsonb, 8, 2, NOW() - INTERVAL '1 hour'),
(5000000004, 1002, 'Has anyone experimented with Redis Sorted Sets for newsfeed pagination?', '[]'::jsonb, 20, 7, NOW() - INTERVAL '30 minutes'),
(5000000005, 1005, 'Hello world from Eva!', '[]'::jsonb, 3, 0, NOW() - INTERVAL '15 minutes')
ON CONFLICT (post_id) DO NOTHING;
