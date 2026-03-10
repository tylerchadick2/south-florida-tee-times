# Hosting South Florida Tee Times (Free)

Run the app in the cloud so you can use it without local hosting. **All functionality is the same**: same server code, same HTML, same APIs and scrapers (ForeUp, Chronogolf, TeeItUp, Club Caddie, Eagle Club, GolfNow).

---

## Step-by-step: Deploy to Render (free)

### Step 1: Put your project on GitHub

1. **Create a GitHub account** (if you don’t have one): [github.com](https://github.com) → Sign up.

2. **Create a new repository**  
   - Click **New** (or **+** → **New repository**).  
   - **Repository name**: e.g. `south-florida-tee-times`.  
   - Leave it **Public**.  
   - Do **not** add a README, .gitignore, or license (you already have files).  
   - Click **Create repository**.

3. **Push your project** from your computer.  
   In Terminal, from your **Projects** folder (where `golf_server.py` and `golf_ui.html` live):

   ```bash
   cd /Users/tylerchadick/Desktop/Projects
   ```

   If this folder is **not** a Git repo yet:

   ```bash
   git init
   git add golf_server.py golf_ui.html requirements.txt Dockerfile .dockerignore
   git commit -m "Add tee times app and Docker for hosting"
   git branch -M main
   git remote add origin https://github.com/YOUR_USERNAME/south-florida-tee-times.git
   git push -u origin main
   ```

   Replace `YOUR_USERNAME` with your GitHub username. If GitHub asks you to log in, use your username and a **Personal Access Token** as the password (Settings → Developer settings → Personal access tokens).

   If the folder **is** already a Git repo, just add and push the files you need:

   ```bash
   git add golf_server.py golf_ui.html requirements.txt Dockerfile .dockerignore
   git commit -m "Add Docker and files for Render"
   git remote add origin https://github.com/YOUR_USERNAME/south-florida-tee-times.git
   git push -u origin main
   ```

   **Required in the repo:** `golf_server.py`, `golf_ui.html`, `requirements.txt`, `Dockerfile`. Optional: `.dockerignore`, `HOSTING.md`.

---

### Step 2: Create a Render account

1. Go to [render.com](https://render.com).  
2. Click **Get Started for Free**.  
3. Sign up with **GitHub** (easiest — Render will then see your repos).  
4. Authorize Render when GitHub asks.  
5. No credit card is required for the free tier.

---

### Step 3: Create a new Web Service

1. In the Render dashboard, click **New +** (top right).  
2. Click **Web Service**.  
3. Under **Connect a repository**, find **south-florida-tee-times** (or the repo name you used).  
   - If you don’t see it, click **Configure account** and connect the right GitHub account or grant access to that repo.  
4. Click **Connect** next to that repository.

---

### Step 4: Configure the service

Fill in the form as follows. Leave anything not mentioned as default.

| Field | Value |
|--------|--------|
| **Name** | `south-florida-tee-times` (or any name; this becomes part of the URL) |
| **Region** | Choose one close to you (e.g. **Oregon (US West)** or **Ohio (US East)**) |
| **Branch** | `main` |
| **Runtime** | **Docker** |
| **Dockerfile Path** | `Dockerfile` (leave as-is if the file is in the repo root) |
| **Instance Type** | **Free** |

Scroll down:

- **Build Command** — leave blank (Docker build is used).  
- **Start Command** — leave blank (the Dockerfile `CMD` runs the app).

**Environment variables (recommended for free tier):**  
Click **Advanced** → **Add Environment Variable**. Add:

| Key | Value | Why |
|-----|--------|-----|
| `MAX_PARALLEL_BROWSERS` | `1` | **Required on Render free tier (512MB).** Use only 1 Chrome at a time so you don’t run out of memory. With `2` or more the instance can be killed (“Ran out of memory”). |
| `PYTHONUNBUFFERED` | `1` | (Optional) Cleaner logs. |

Render already sets `PORT`.

---

### Step 5: Deploy

1. Click **Create Web Service**.  
2. Render will **clone the repo**, **build the Docker image** (installing Python, Chrome, and dependencies), and **start the app**.  
3. The first build usually takes **3–8 minutes** (Chrome and pip installs).  
4. Watch the **Logs** tab. When you see something like “Running on http://0.0.0.0:XXXX”, the app is up.  
5. At the top of the page, you’ll see the service URL, e.g. **https://south-florida-tee-times.onrender.com**.  
6. Click that URL (or open it in a new tab). You should see the South Florida Tee Times page.  
7. Pick a date, click **Check All Courses**, and confirm that tee times load (same behavior as local).

---

### Step 6: Use the app

- **Bookmark** the Render URL.  
- **First time after 15+ minutes of no use:** the service may be asleep; the first load can take **30–60 seconds**, then the page appears.  
- **After that:** use it as normal; “Check All Courses” and all features work the same as when running locally.

---

### If something goes wrong

- **Build fails:**  
  - Open the **Logs** tab and read the error.  
  - Confirm the repo has `golf_server.py`, `golf_ui.html`, `requirements.txt`, and `Dockerfile` in the root.  
  - Fix any errors, commit, and push; Render will rebuild automatically.

- **“Application failed to respond” or 503:**  
  - Free tier may still be starting (cold start). Wait 1–2 minutes and try again.  
  - Check Logs for crashes (e.g. Chrome or port binding issues).

- **Page loads but “Check All Courses” does nothing or errors:**  
  - Open the browser’s Developer Tools (F12) → **Console** and **Network** tabs.  
  - Confirm requests go to your Render URL (same origin). If you see CORS or 404 errors, the backend may not be up; check Render Logs.

---

### Render free tier notes

- **Spins down** after ~15 minutes of no traffic. The **first** request after that can take ~30–60 seconds (cold start); after that it’s fast.  
- **750 hours/month** free — enough for one service running 24/7.  
- **“Instance failed: Ran out of memory (used over 512MB)”** — Render free tier has a 512MB limit. Set **`MAX_PARALLEL_BROWSERS=1`** so only one Chrome runs at a time. The app also uses lighter Chrome settings to stay under the limit.  
- No code changes are required: the same repo works locally and on Render.

## Option 2: Run Docker locally (same image as production)

To test the exact image used in production:

```bash
cd /path/to/Projects
docker build -t golf-tee-times .
docker run -p 5000:5000 -e PORT=5000 golf-tee-times
```

Then open **http://localhost:5000**. Behavior matches hosted deployment.

## What stays the same

- **Server**: Same `golf_server.py` (PORT and HOST read from env; already supported).
- **UI**: Same `golf_ui.html` (uses same-origin API when host is not localhost).
- **Scrapers**: Same Selenium/Chrome flow; Chrome is installed in the Docker image.
- **Endpoints**: `/`, `/api/courses`, `/api/teetimes`, `/api/all_teetimes`, `/api/all_teetimes_stream` — all unchanged.

No separate “production” branch of code: one codebase runs locally or hosted.
