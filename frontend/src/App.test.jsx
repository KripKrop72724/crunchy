import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import App from './App';
import { vi, expect } from 'vitest';

global.WebSocket = class {
  constructor(url) {
    this.url = url;
    setTimeout(() => {
      this.onmessage &&
        this.onmessage({
          data: JSON.stringify({
            status: 'completed',
            uploaded: 1,
            total: 1,
            rows: 1,
            table: 'import_job_1',
          }),
        });
    }, 0);
  }
  close() {}
};

  vi.mock('axios', () => {
    const postMock = vi.fn((url, data) => {
      if (url.endsWith('/upload')) {
        return Promise.resolve({ data: { job_id: 'job-1' } });
      }
      if (url.includes('/query')) {
        if (data.filters && data.filters.length) {
          return Promise.resolve({ data: { rows: [{ col1: 'filtered' }], total: 1 } });
        }
        return Promise.resolve({ data: { rows: [{ col1: 'original' }], total: 1 } });
      }
      return Promise.resolve({ data: {} });
    });
    const getMock = vi.fn(() => Promise.resolve({ data: { columns: ['col1'] } }));
    return {
      default: {
        post: postMock,
        get: getMock,
      },
    };
  });

test('uploads file, shows rows and applies filter', async () => {
  render(<App />);
  const apiInput = screen.getByPlaceholderText('API Key');
  fireEvent.change(apiInput, { target: { value: 'testkey' } });

  const file = new File(['hello'], 'hello.csv', { type: 'text/csv' });
  const fileInput = screen.getByTestId('file-input');
  fireEvent.change(fileInput, { target: { files: [file] } });

  fireEvent.click(screen.getByText('Upload'));

  await waitFor(() => screen.getByText(/Status: completed/i));
  expect(screen.getByText(/Rows: 1/)).toBeInTheDocument();

  await waitFor(() => screen.getByText('original'));

  fireEvent.click(screen.getByText('Add Filter'));
  fireEvent.change(screen.getByTestId('column-0'), { target: { value: 'col1' } });
  fireEvent.change(screen.getByTestId('value-0'), { target: { value: 'x' } });
  fireEvent.click(screen.getByText('Apply Filters'));

  await waitFor(() => screen.getByText('filtered'));
});
