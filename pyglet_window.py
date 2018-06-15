from pyglet.gl import *
import time

class Triangle:
    def __init__(self):

        self.figures = []
        o_x = .0
        o_y = .0
        z = .0
        offset = .15
        offset_thick = .05  


        arrow_sx = [o_x-.2,     o_y,         z, 
                    o_x+.0,     o_y+.5,     .0,
                    o_x+.1,     o_y+.5,     .0,
                    o_x-.1,     o_y,        .0,
                    o_x+.1,     o_y-.5,     .0,
                    o_x,        o_y-.5,     .0]


        arrow_dx = [o_x+.2,     o_y,         z,
                    o_x+.0,     o_y+.5,     .0,
                    o_x-.1,     o_y+.5,     .0,
                    o_x+.1,     o_y,        .0,
                    o_x-.1,     o_y-.5,     .0,
                    o_x,        o_y-.5,     .0]
        
        arrow_down = [-(o_y),               +(o_x-.2),                   z, 
                      -(o_y+.5 -offset),    +(o_x+.0),                  .0,
                      -(o_y+.5 -offset),    +(o_x+.1+offset_thick),     .0,
                      -(o_y),               +(o_x-.1+offset_thick),     .0,
                      -(o_y-.5 +offset),    +(o_x+.1+offset_thick),     .0,
                      -(o_y-.5 +offset),    +(o_x),                     .0]


        arrow_up = [+(o_y),               -(o_x-.2),                   z, 
                    +(o_y+.5 -offset),    -(o_x+.0),                  .0,
                    +(o_y+.5 -offset),    -(o_x+.1+offset_thick),     .0,
                    +(o_y),               -(o_x-.1+offset_thick),     .0,
                    +(o_y-.5 +offset),    -(o_x+.1+offset_thick),     .0,
                    +(o_y-.5 +offset),    -(o_x),                     .0]     

        self.vertices = pyglet.graphics.vertex_list(6, ('v3f', arrow_sx),
                                                       ('c3B', [100,200,220, 100,200,220, 100,200,220, 100,200,220, 100,200,220, 100,200,220]))



        self.vertices_2 = pyglet.graphics.vertex_list(6, ('v3f', arrow_dx), 
                                                            ('c3b', [100,00,220, 100,00,220, 100,00,220, 100,00,220, 100,00,220, 100,200,020]))

        #self.vertices_2 = pyglet.graphics.vertex_list(5, ('v3f', [0.5,0.5,0, 0.2,-0.2,0, 0.1,1.5,0, -0.2,0.3,0, 0.2,-0.3,0.0]),
         #                                               ('c3B', [200,00,220, 0,110,192, 129,0,238, 77,87,81, 1,22,222]))

        self.figures.append(self.vertices)
        self.figures.append(self.vertices_2)



class MyWindow(pyglet.window.Window):
        def __init__(self, *args, **kwargs):
            super(MyWindow, self).__init__(*args, **kwargs)
            self.set_minimum_size(400,300)
            glClearColor(0, 0, 0, 0)
            pyglet.clock.schedule_interval(self.update, 1.0/24.0)
            self.triangle = Triangle()

        def on_draw(self):
            self.clear()

            for fig in self.triangle.figures:
                fig.draw(pyglet.gl.GL_POLYGON)
                #fig.draw(pyglet.gl.GL_POLYGON)

        def on_resize(self, width, height):
            glViewport(0, 0, width, height)

        def update(self, dt):
            pass


if __name__ == "__main__":
    window = MyWindow(1280, 720, "test directions", resizable=False)
    #window.on_draw()
    pyglet.app.run()