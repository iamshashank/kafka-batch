# frozen_string_literal: true

require "digest"
require "fileutils"
require "json"
require "time"

module KafkaBatch
  module Ai
    # Offline chunker for ai/README.md + ai/FAQ.md.
    # Produces a packaged JSON artifact so boot-time Redis sync never re-parses
    # markdown on every pod — only loads the prebuilt file.
    #
    # Strategy:
    #   - README: one chunk per ### section (with parent ## title); intro text
    #     under a ## before the first ### is its own chunk; ##-only sections
    #     without ### become one chunk.
    #   - FAQ: one chunk per ### question; section letter kept in metadata.
    #   - Oversized chunks (> max_chars) split on blank lines.
    class Chunker
      MAX_CHARS_DEFAULT = 2_800

      Chunk = Struct.new(
        :id, :source, :title, :heading_path, :section, :text, :char_count,
        keyword_init: true
      )

      class << self
        # @param readme_path [String]
        # @param faq_path [String]
        # @param max_chars [Integer]
        # @return [Hash] serializable manifest + chunks
        def build(readme_path:, faq_path:, max_chars: MAX_CHARS_DEFAULT)
          readme = File.read(readme_path)
          faq    = File.read(faq_path)
          chunks = []
          chunks.concat(chunk_readme(readme, max_chars: max_chars))
          chunks.concat(chunk_faq(faq, max_chars: max_chars))

          payload = {
            "format"         => 1,
            "built_at"       => Time.now.utc.iso8601,
            "max_chars"      => max_chars,
            "readme_sha256"  => Digest::SHA256.hexdigest(readme),
            "faq_sha256"     => Digest::SHA256.hexdigest(faq),
            "chunk_count"    => chunks.size,
            "chunks"         => chunks.map { |c| chunk_to_h(c) }
          }
          payload["corpus_version"] = Digest::SHA256.hexdigest(
            payload["readme_sha256"] + ":" + payload["faq_sha256"] + ":" +
              payload["chunks"].map { |c| c["id"] + c["text"] }.join
          )[0, 32]
          payload
        end

        def write!(output_path:, readme_path:, faq_path:, max_chars: MAX_CHARS_DEFAULT)
          payload = build(readme_path: readme_path, faq_path: faq_path, max_chars: max_chars)
          FileUtils.mkdir_p(File.dirname(output_path))
          File.write(output_path, JSON.pretty_generate(payload) + "\n")
          payload
        end

        private

        def chunk_to_h(c)
          {
            "id"           => c.id,
            "source"       => c.source,
            "title"        => c.title,
            "heading_path" => c.heading_path,
            "section"      => c.section,
            "text"         => c.text,
            "char_count"   => c.char_count
          }
        end

        def chunk_readme(markdown, max_chars:)
          sections = split_by_heading(markdown, /^##\s+(.+)$/)
          out = []
          sections.each do |sec|
            title = sec[:title]
            body  = sec[:body]
            title = "Introduction" if title.to_s.strip.empty?
            next if body.to_s.strip.empty?
            next if title =~ /\Atable of contents\z/i

            subsections = split_by_heading(body, /^###\s+(.+)$/)
            if subsections.size == 1 && subsections[0][:title].nil?
              emit_splits(out, source: "readme", title: title, heading_path: [title],
                          section: title, text: format_chunk(title, body), max_chars: max_chars)
            else
              intro = subsections[0]
              if intro[:title].nil? && !intro[:body].to_s.strip.empty?
                emit_splits(out, source: "readme", title: title, heading_path: [title],
                            section: title, text: format_chunk(title, intro[:body]), max_chars: max_chars)
              end
              subsections.each do |sub|
                next if sub[:title].nil?

                path = [title, sub[:title]]
                emit_splits(out, source: "readme", title: sub[:title], heading_path: path,
                            section: title, text: format_chunk(path.join(" › "), sub[:body]),
                            max_chars: max_chars)
              end
            end
          end
          out
        end

        def chunk_faq(markdown, max_chars:)
          sections = split_by_heading(markdown, /^##\s+(.+)$/)
          out = []
          sections.each do |sec|
            section_title = sec[:title]
            next if section_title.to_s.strip.empty?

            split_by_heading(sec[:body], /^###\s+(.+)$/).each do |q|
              next if q[:title].nil?

              path = [section_title, q[:title]]
              emit_splits(out, source: "faq", title: q[:title], heading_path: path,
                          section: section_title,
                          text: format_chunk("FAQ: #{q[:title]}", q[:body]),
                          max_chars: max_chars)
            end
          end
          out
        end

        # @return [Array<Hash>] { title:, body: } — first may have title nil (preamble)
        def split_by_heading(text, pattern)
          lines = text.to_s.lines
          parts = []
          current_title = nil
          buf = []
          lines.each do |line|
            if (m = line.match(pattern))
              parts << { title: current_title, body: buf.join }
              current_title = m[1].strip
              buf = []
            else
              buf << line
            end
          end
          parts << { title: current_title, body: buf.join }
          parts
        end

        def format_chunk(heading, body)
          body = body.to_s.strip
          return heading.to_s if body.empty?

          "#{heading}\n\n#{body}"
        end

        def emit_splits(out, source:, title:, heading_path:, section:, text:, max_chars:)
          pieces = split_text(text, max_chars)
          pieces.each_with_index do |piece, idx|
            suffix = pieces.size > 1 ? "-p#{idx + 1}" : ""
            id = stable_id(source, heading_path.join("|") + suffix)
            out << Chunk.new(
              id: id,
              source: source,
              title: pieces.size > 1 ? "#{title} (part #{idx + 1})" : title,
              heading_path: heading_path,
              section: section,
              text: piece,
              char_count: piece.length
            )
          end
        end

        def split_text(text, max_chars)
          text = text.to_s.strip
          return [] if text.empty?
          return [text] if text.length <= max_chars

          paras = text.split(/\n{2,}/)
          chunks = []
          buf = +""
          paras.each do |para|
            candidate = buf.empty? ? para : "#{buf}\n\n#{para}"
            if candidate.length <= max_chars
              buf = candidate
            else
              chunks << buf unless buf.empty?
              if para.length <= max_chars
                buf = para
              else
                # Hard-split very long paragraphs
                para.scan(/.{1,#{max_chars}}/m) { |slice| chunks << slice }
                buf = +""
              end
            end
          end
          chunks << buf unless buf.empty?
          chunks
        end

        def stable_id(source, material)
          digest = Digest::SHA256.hexdigest("#{source}:#{material}")[0, 16]
          slug = material.downcase.gsub(/[^a-z0-9]+/, "-")[0, 48].gsub(/^-|-$/, "")
          "#{source}-#{slug}-#{digest}"
        end
      end
    end
  end
end
